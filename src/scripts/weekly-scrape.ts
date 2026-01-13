import { getBulkRuns } from '../api.js';
import { createTable, insertRun } from '../db.js';
import { formatInTimeZone } from 'date-fns-tz';

const JURISDICTIONS = ['VIC', 'NSW', 'QLD', 'SA', 'WA', 'TAS', 'ACT', 'NT', 'NZ'];
const SYDNEY_TIMEZONE = 'Australia/Sydney';

const SCRAPE_YEAR = 2025;
const SCRAPE_MONTH = 12;

async function scrapeMonth() {
  console.log('[MONTHLY SCRAPE] Starting monthly scrape...');

  const year = SCRAPE_YEAR;
  const month = SCRAPE_MONTH;
  
  const monthDate = new Date(year, month - 1, 1);
  const monthStr = formatInTimeZone(monthDate, SYDNEY_TIMEZONE, 'yyyy-MM');

  console.log(`[MONTHLY SCRAPE] Scraping for month: ${monthStr}`);

  const db = await createTable();
  let totalRuns = 0;

  for (const jur of JURISDICTIONS) {
    console.log(`\n[MONTHLY SCRAPE] Processing ${jur}...`);
    try {
      const runs = await getBulkRuns(jur, year, month);
      if (runs && runs.length > 0) {
        for (const run of runs) {
          await insertRun(db, run);
        }
        totalRuns += runs.length;
        console.log(`  ✓ ${jur}: Inserted ${runs.length} runs`);
      } else {
        console.log(`  - ${jur}: No data`);
      }
    } catch (error: any) {
      console.error(`  ✗ Error fetching ${jur} ${monthStr}:`, error.message);
    }
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  console.log(`\n[MONTHLY SCRAPE] ✅ Complete!`);
  console.log(`[MONTHLY SCRAPE] Total runs inserted: ${totalRuns}`);
  console.log(`[MONTHLY SCRAPE] Month: ${monthStr}`);

  process.exit(0);
}

scrapeMonth().catch((error) => {
  console.error('[MONTHLY SCRAPE] ❌ Fatal error:', error);
  process.exit(1);
});
