import { formatInTimeZone } from 'date-fns-tz';
import { getMeetings, getRacesForMeeting } from '../api.js';
import {
  openDb,
  getDogStats, getTrainerStats, getBoxBiasStats,
  getRecentPerformanceStats, getLastRaceGrades,
  getRunningStyleStats, getTrackSpecificStats, getDistanceSpecificStats,
  getBoxPerformanceByDog, getOne
} from '../db.js';
import { saveDailyRaces } from '../supabase.js';

const JURISDICTIONS = ['VIC', 'NSW', 'QLD', 'SA', 'WA', 'TAS', 'ACT', 'NT', 'NZ'];
const SYDNEY_TIMEZONE = 'Australia/Sydney';

function determineRunningStyle(avgFirstSplitPos: number | null, leadRate: number | null): string {
  if (avgFirstSplitPos === null || leadRate === null) return 'Unknown';
  if (leadRate >= 0.35 || avgFirstSplitPos <= 1.8) return 'Early';
  if (avgFirstSplitPos <= 4.0) return 'Mid';
  return 'Close';
}

function determineBoxPreference(boxNumber: number, boxPreferenceData: any[] | null): string {
  if (!boxNumber || !boxPreferenceData || boxPreferenceData.length === 0) return 'Unknown';
  const currentBoxGroup = boxNumber <= 2 ? 'inside' : (boxNumber <= 5 ? 'middle' : 'outside');
  const bestGroup = boxPreferenceData.reduce((best: any, curr: any) => 
    (!best || curr.winRate > best.winRate) ? curr : best
  , null);
  if (!bestGroup) return 'Unknown';
  if (bestGroup.boxGroup === currentBoxGroup) return 'Good';
  const currentGroupData = boxPreferenceData.find((g: any) => g.boxGroup === currentBoxGroup);
  if (!currentGroupData) return 'Neutral';
  const diff = bestGroup.winRate - currentGroupData.winRate;
  return diff > 0.10 ? 'Poor' : 'Neutral';
}

function getGradeValue(grade: string | null): number {
  if (!grade) return 99;
  const g = grade.toLowerCase();
  if (g.includes('group') || g.includes('free') || g.includes('open') || g.includes('special')) return 1;
  if (g.includes('1')) return 1;
  if (g.includes('2')) return 2;
  if (g.includes('3')) return 3;
  if (g.includes('4')) return 4;
  if (g.includes('5')) return 5;
  if (g.includes('6') || g.includes('7') || g.includes('maiden') || g.includes('m')) return 6;
  return 5;
}

function calculateClassDrop(lastGrade: string | null, currentGrade: string | null): boolean {
  if (!lastGrade || !currentGrade) return false;
  const lastVal = getGradeValue(lastGrade);
  const curVal = getGradeValue(currentGrade);
  return lastVal < curVal;
}

async function computeDaily() {
  const todayStr = formatInTimeZone(new Date(), SYDNEY_TIMEZONE, 'yyyy-MM-dd');
  
  console.log(`[DAILY COMPUTE] Computing daily races for ${todayStr}...`);

  console.log('[DAILY COMPUTE] Step 1/5: Fetching meetings from API...');
  const allMeetingsByJur: { [key: string]: any[] } = {};
  const meetingPromises = JURISDICTIONS.map(async (jur) => {
    try {
      const meetings = await getMeetings(todayStr, todayStr, jur);
      allMeetingsByJur[jur] = meetings || [];
      if (meetings && meetings.length > 0) {
        console.log(`  ✓ ${jur}: Found ${meetings.length} meeting(s)`);
      }
    } catch (error: any) {
      console.error(`  ✗ Failed to get meetings for ${jur}:`, error.message);
      allMeetingsByJur[jur] = [];
    }
  });
  await Promise.all(meetingPromises);

  const allMeetings: any[] = [].concat(...Object.values(allMeetingsByJur) as any);
  console.log(`[DAILY COMPUTE] Step 2/5: Fetching races for ${allMeetings.length} meeting(s)...`);
  
  const racePromises = allMeetings.map(async (meeting, index) => {
    try {
      const races = await getRacesForMeeting(meeting.meetingId);
      await new Promise(resolve => setTimeout(resolve, 250));
      if (index % 5 === 0) {
        console.log(`  Progress: ${index + 1}/${allMeetings.length} meetings processed`);
      }
      return { meetingId: meeting.meetingId, races: races || [] };
    } catch (error) {
      console.error(`  ✗ Failed to get races for meeting ${meeting.meetingId}`);
      return { meetingId: meeting.meetingId, races: [], error: 'Failed to load races' };
    }
  });
  const raceResults = await Promise.all(racePromises);
  console.log(`  ✓ Fetched races for all meetings`);

  const racesByMeetingId = new Map();
  raceResults.forEach(result => {
    racesByMeetingId.set(result.meetingId, result);
  });

  const allData: { [key: string]: any[] } = {};
  for (const jur in allMeetingsByJur) {
    allData[jur] = allMeetingsByJur[jur]!.map(meeting => {
      const raceResult = racesByMeetingId.get(meeting.meetingId);
      return {
        ...meeting,
        races: raceResult ? raceResult.races : [],
        error: raceResult ? raceResult.error : undefined
      };
    });
  }

  console.log("[DAILY COMPUTE] Step 3/5: Computing statistics from local SQLite...");
  const db = await openDb();
  console.log("  ✓ Database opened successfully");
  
  const testResult = getOne(db, "SELECT COUNT(*) as count FROM runs");
  console.log(`  ✓ Database has ${testResult.count} total runs`);
  
  const startTime = Date.now();
  
  const dogStats = await getDogStats(db);
  const trainerStats = await getTrainerStats(db);
  const boxBiasStats = await getBoxBiasStats(db);
  const recentPerfStats = await getRecentPerformanceStats(db);
  const lastRaceGrades = await getLastRaceGrades(db);
  const runningStyleStats = await getRunningStyleStats(db);
  const trackSpecificStats = await getTrackSpecificStats(db);
  const distanceSpecificStats = await getDistanceSpecificStats(db);
  const boxPerformanceStats = await getBoxPerformanceByDog(db);
  
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`  ✓ Computed all statistics in ${elapsed}s`);

  const dogStatsMap = new Map(dogStats.map((r: any) => [r.dogId, {
    totalStarts: r.totalStarts,
    winRate: r.totalStarts > 0 ? r.wins / r.totalStarts : 0,
    placeRate: r.totalStarts > 0 ? r.places / r.totalStarts : 0
  }]));
  const trainerStatsMap = new Map(trainerStats.map((r: any) => [r.trainerId, {
    trainerStrikeRate: r.totalStarts > 0 ? r.wins / r.totalStarts : 0
  }]));
  const boxBiasStatsMap = new Map(boxBiasStats.map((r: any) => 
    [`${r.trackCode}-${r.boxNumber}`, {
      boxWinPercentage: r.totalStarts > 0 ? r.wins / r.totalStarts : 0
    }]));
  const recentPerfStatsMap = new Map(recentPerfStats.map((r: any) => 
    [`${r.dogId}-${r.trackCode}-${r.distanceInMetres}`, {
      avgTimeLast5TrackDist: r.avgTimeLast5TrackDist,
      avgSplitLast5TrackDist: r.avgSplitLast5TrackDist
    }]));
  const lastRaceGradeMap = new Map(lastRaceGrades.map((r: any) => [r.dogId, r.lastRaceOutgoingGrade]));
  const runningStyleMap = new Map(runningStyleStats.map((r: any) => [r.dogId, {
    avgFirstSplitPosition: r.avgFirstSplitPosition,
    avgFirstSplitPositionL5: r.avgFirstSplitPositionL5,
    leadAtFirstBendRate: r.leadAtFirstBendRate
  }]));
  
  const trackSpecificMap = new Map();
  trackSpecificStats.forEach((r: any) => {
    trackSpecificMap.set(`${r.dogId}-${r.trackCode}`, {
      startsAtTrack: r.startsAtTrack,
      winRateAtTrack: r.winRateAtTrack,
      placeRateAtTrack: r.placeRateAtTrack
    });
  });

  const distanceSpecificMap = new Map();
  distanceSpecificStats.forEach((r: any) => {
    distanceSpecificMap.set(`${r.dogId}-${r.distanceInMetres}`, {
      startsAtDistance: r.startsAtDistance,
      winRateAtDistance: r.winRateAtDistance,
      placeRateAtDistance: r.placeRateAtDistance
    });
  });

  const boxPerformanceMap = new Map();
  boxPerformanceStats.forEach((r: any) => {
    if (!boxPerformanceMap.has(r.dogId)) {
      boxPerformanceMap.set(r.dogId, []);
    }
    boxPerformanceMap.get(r.dogId).push({
      boxGroup: r.boxGroup,
      starts: r.starts,
      winRate: r.winRate,
      avgTime: r.avgTime
    });
  });

  console.log("[DAILY COMPUTE] Step 4/5: Merging race data with statistics...");
  let totalRunners = 0;
  for (const jur in allData) {
    for (const meeting of allData[jur]!) {
      if (meeting.races) {
        for (const race of meeting.races) {
          if (race.runs) {
            totalRunners += race.runs.length;
            race.runs = race.runs.map((run: any) => {
              const { dogId, trainerId, boxNumber } = run;
              const { trackCode, distance } = { trackCode: meeting.trackCode, distance: race.distance };

              const dogStat = dogStatsMap.get(dogId);
              const trainerStat = trainerStatsMap.get(trainerId);
              const boxStat = boxBiasStatsMap.get(`${trackCode}-${boxNumber}`);
              const recentStat = recentPerfStatsMap.get(`${dogId}-${trackCode}-${distance}`);
              const lastGrade = lastRaceGradeMap.get(dogId);
              const runningStyle = runningStyleMap.get(dogId);
              const trackSpecific = trackSpecificMap.get(`${dogId}-${trackCode}`);
              const distanceSpecific = distanceSpecificMap.get(`${dogId}-${distance}`);
              const boxPreferenceData = boxPerformanceMap.get(dogId);

              return {
                ...run,
                totalStarts: dogStat?.totalStarts ?? null,
                winRate: dogStat?.winRate ?? null,
                placeRate: dogStat?.placeRate ?? null,
                trainerStrikeRate: trainerStat?.trainerStrikeRate ?? null,
                boxWinPercentage: boxStat?.boxWinPercentage ?? null,
                avgTimeLast5TrackDist: recentStat?.avgTimeLast5TrackDist ?? null,
                avgSplitLast5TrackDist: recentStat?.avgSplitLast5TrackDist ?? null,
                classChange: run.incomingGrade && lastGrade 
                  ? `${lastGrade} -> ${run.incomingGrade}` 
                  : (run.incomingGrade ? `Debut -> ${run.incomingGrade}` : null),
                isDownGrade: calculateClassDrop(lastGrade, run.incomingGrade),
                runningStyle: determineRunningStyle(
                  runningStyle?.avgFirstSplitPositionL5 ?? null,
                  runningStyle?.leadAtFirstBendRate ?? null
                ),
                leadAtFirstBendRate: runningStyle?.leadAtFirstBendRate ?? null,
                avgFirstSplitPosition: runningStyle?.avgFirstSplitPosition ?? null,
                winRateAtTrack: trackSpecific?.winRateAtTrack ?? null,
                placeRateAtTrack: trackSpecific?.placeRateAtTrack ?? null,
                startsAtTrack: trackSpecific?.startsAtTrack ?? null,
                winRateAtDistance: distanceSpecific?.winRateAtDistance ?? null,
                placeRateAtDistance: distanceSpecific?.placeRateAtDistance ?? null,
                startsAtDistance: distanceSpecific?.startsAtDistance ?? null,
                boxPreference: determineBoxPreference(boxNumber, boxPreferenceData),
              };
            });
          }
        }
      }
    }
  }
  console.log(`  ✓ Added ${totalRunners} runners with statistics`);

  console.log("[DAILY COMPUTE] Step 5/5: Saving to Supabase daily_races table...");
  await saveDailyRaces(todayStr, allData);
  console.log(`  ✓ Saved to Supabase`);
  
  console.log(`\n[DAILY COMPUTE] ✅ Successfully completed!`);
  console.log(`[DAILY COMPUTE] Daily races for ${todayStr} are now available in Supabase.`);
  
  process.exit(0);
}

computeDaily().catch((error) => {
  console.error('[DAILY COMPUTE] ❌ Fatal error:', error);
  process.exit(1);
});
