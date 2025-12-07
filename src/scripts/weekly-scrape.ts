import { getBulkRunsByDay } from '../api.js';
import { createTable, insertRun } from '../db.js';
import { formatInTimeZone } from 'date-fns-tz';

const JURISDICTIONS = ['VIC', 'NSW', 'QLD', 'SA', 'WA', 'TAS', 'ACT', 'NT', 'NZ'];
const SYDNEY_TIMEZONE = 'Australia/Sydney';

async function scrapeWeek() {
  console.log('[WEEKLY SCRAPE] Starting weekly scrape...');
  
  const today = new Date();
  const DAYS_TO_SCRAPE = 8;
  
  const dates = [];
  for (let i = DAYS_TO_SCRAPE; i >= 1; i--) {
    const date = new Date(today);
    date.setDate(today.getDate() - i);
    dates.push({
      year: date.getFullYear(),
      month: date.getMonth() + 1,
      day: date.getDate(),
      dateStr: formatInTimeZone(date, SYDNEY_TIMEZONE, 'yyyy-MM-dd')
    });
  }
  
  const startStr = dates[0]!.dateStr;
  const endStr = dates[dates.length - 1]!.dateStr;
  
  console.log(`[WEEKLY SCRAPE] Scraping last ${DAYS_TO_SCRAPE} days: ${startStr} to ${endStr}`);
  
  const db = await createTable();
  let totalRuns = 0;
  let completedRequests = 0;
  
  const totalRequests = dates.length * JURISDICTIONS.length;
  console.log(`[WEEKLY SCRAPE] Total requests: ${dates.length} days × ${JURISDICTIONS.length} jurisdictions = ${totalRequests}`);
  
  for (const jur of JURISDICTIONS) {
    console.log(`\n[WEEKLY SCRAPE] Processing ${jur}...`);
    
    for (const date of dates) {
      try {
        const runs = await getBulkRunsByDay(jur, date.year, date.month, date.day);
        
        if (runs && runs.length > 0) {
          for (const run of runs) {
            await insertRun(db, run);
          }
          totalRuns += runs.length;
          console.log(`  ✓ ${date.dateStr} ${jur}: Inserted ${runs.length} runs`);
        } else {
          console.log(`  - ${date.dateStr} ${jur}: No data`);
        }
        
        completedRequests++;
        if (completedRequests % 10 === 0) {
          console.log(`  Progress: ${completedRequests}/${totalRequests} (${Math.round(completedRequests/totalRequests*100)}%)`);
        }
        
        await new Promise(resolve => setTimeout(resolve, 300));
        
      } catch (error: any) {
        console.error(`  ✗ Error fetching ${jur} ${date.dateStr}:`, error.message);
        completedRequests++;
      }
    }
  }
  
  console.log(`\n[WEEKLY SCRAPE] ✅ Complete!`);
  console.log(`[WEEKLY SCRAPE] Total runs inserted: ${totalRuns}`);
  console.log(`[WEEKLY SCRAPE] Week: ${startStr} to ${endStr}`);
  
  process.exit(0);
}

scrapeWeek().catch((error) => {
  console.error('[WEEKLY SCRAPE] ❌ Fatal error:', error);
  process.exit(1);
});
