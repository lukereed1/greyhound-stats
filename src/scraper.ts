import { getBulkRuns, getBulkRunsByDay } from './api.js';
import { createTable, insertRun } from './db.js';

const JURISDICTIONS = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA', 'NZ'];
const YEARS_TO_SCRAPE = 6; // How many years back to scrape
const MONTHS_BEFORE_CURRENT = 3; // stops 3 month before curr data for bulk runs by years

const TARGET_YEAR = 2025;  // Which year to scrape for bulkByMonth
const TARGET_MONTH = 11;    // Which month to scrape for bulkByMonth

const getDateRangesToScrape = () => {
  const cutoffDate = new Date();
  cutoffDate.setMonth(cutoffDate.getMonth() - MONTHS_BEFORE_CURRENT);
  
  const startDate = new Date();
  startDate.setFullYear(startDate.getFullYear() - YEARS_TO_SCRAPE);
  
  const ranges = [];
  
  const currentDate = new Date(startDate);
  
  while (currentDate <= cutoffDate) {
    ranges.push({
      year: currentDate.getFullYear(),
      month: currentDate.getMonth() + 1
    });
    
    currentDate.setMonth(currentDate.getMonth() + 1);
  }
  
  return ranges;
}

const getDaysInMonth = (year: number, month: number) => {
  const daysInMonth = new Date(year, month, 0).getDate();
  const days = [];
  
  for (let day = 1; day <= daysInMonth; day++) {
    days.push(day);
  }
  
  return days;
}

export const getBulkRunsByYears = (async () => {
  console.log(`Jurisdictions: ${JURISDICTIONS.join(', ')}`);
  console.log(`Scraping ${YEARS_TO_SCRAPE} years back, stopping ${MONTHS_BEFORE_CURRENT} months before current date`);
  
  const db = await createTable();
  const dateRanges = getDateRangesToScrape();
  
  console.log(`Total periods to scrape: ${dateRanges.length} months × ${JURISDICTIONS.length} jurisdictions = ${dateRanges.length * JURISDICTIONS.length} requests\n`);
  
  let totalRuns = 0;
  let completedRequests = 0;
  const totalRequests = dateRanges.length * JURISDICTIONS.length;
  
  for (const jurisdiction of JURISDICTIONS) {
    console.log(`\n=== Scraping ${jurisdiction} ===`);
    
    for (const { year, month } of dateRanges) {
      try {
        console.log(`Fetching ${jurisdiction} ${year}-${month.toString().padStart(2, '0')}`);
        const runs = await getBulkRuns(jurisdiction, year, month);
        
        if (runs && runs.length > 0) {
          for (const run of runs) {
            await insertRun(db, run);
          }
          totalRuns += runs.length;
          console.log(`   Inserted ${runs.length} runs`);
        } else {
          console.log(`   No data found`);
        }
        
        completedRequests++;
        console.log(`Progress: ${completedRequests}/${totalRequests} (${Math.round(completedRequests/totalRequests*100)}%)`);
        
        // Small delay to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, 500));
        
      } catch (error) {
        console.error(`   Error fetching ${jurisdiction} ${year}-${month}:`, error);
      }
    }
  }
  
  console.log(`New runs inserted: ${totalRuns}`);
  
  await db.close();  
});

export const getBulkRunsByMonth = (async () => {
  console.log(`Scraping ${TARGET_YEAR}-${TARGET_MONTH} (one month only)`);
  console.log(`Jurisdictions: ${JURISDICTIONS.join(', ')}`);
  
  const db = await createTable();
  const allDays = getDaysInMonth(TARGET_YEAR, TARGET_MONTH);
  
  // Get yesterday's date
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  
  // Filter days to only include up to yesterday
  const days = allDays.filter(day => {
    const currentDate = new Date(TARGET_YEAR, TARGET_MONTH - 1, day);
    return currentDate <= yesterday;
  });
  
  console.log(`Total days in month: ${allDays.length}`);
  console.log(`Days to scrape (up to yesterday): ${days.length}`);
  console.log(`Total requests: ${days.length} days × ${JURISDICTIONS.length} jurisdictions = ${days.length * JURISDICTIONS.length}\n`);
  
  let totalRuns = 0;
  let completedRequests = 0;
  const totalRequests = days.length * JURISDICTIONS.length;
  
  for (const jurisdiction of JURISDICTIONS) {
    console.log(`\n=== Scraping ${jurisdiction} ===`);
    
    for (const day of days) {
      try {
        const dateStr = `${TARGET_YEAR}-${TARGET_MONTH.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`;
        console.log(`Fetching ${jurisdiction} ${dateStr}`);
        
        const runs = await getBulkRunsByDay(jurisdiction, TARGET_YEAR, TARGET_MONTH, day);
        
        if (runs && runs.length > 0) {
          for (const run of runs) {
            await insertRun(db, run);
          }
          totalRuns += runs.length;
          console.log(`   Inserted ${runs.length} runs`);
        } else {
          console.log(`   No data found`);
        }
        
        completedRequests++;
        console.log(`Progress: ${completedRequests}/${totalRequests} (${Math.round(completedRequests/totalRequests*100)}%)`);
        
        // Small delay to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, 500));
        
      } catch (error) {
        console.error(`   Error fetching ${jurisdiction} ${TARGET_YEAR}-${TARGET_MONTH}-${day}:`, error);
      }
    }
  }
  
  console.log('\n=== Summary ===');  
  console.log(`New runs inserted: ${totalRuns}`);
  
  await db.close();  
});

// getBulkRunsByYears();

getBulkRunsByMonth();