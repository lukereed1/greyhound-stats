import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { formatInTimeZone } from 'date-fns-tz';
import { getMeetings, getRacesForMeeting } from './api.js';
import {
  openDb,
  getDogStats, getTrainerStats, getBoxBiasStats,
  getRecentPerformanceStats, getLastRaceGrades,
  getRunningStyleStats, getTrackSpecificStats, getDistanceSpecificStats,
  getBoxPerformanceByDog
} from './db.js';
import { getLatestDailyRaces } from './supabase.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = 3000;
const JURISDICTIONS = ['VIC', 'NSW', 'QLD', 'SA', 'WA', 'TAS', 'ACT', 'NT', 'NZ'];

app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    message: 'Greyhound Stats API is running',
    timestamp: new Date().toISOString(),
    endpoints: {
      races: '/api/daily-races',
      todayAll: '/api/races/today/all'
    }
  });
});

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

app.get('/api/races/today/all', async (req, res) => {
  let db;
  try {
    const SYDNEY_TIMEZONE = 'Australia/Sydney';
    const todayStr = formatInTimeZone(new Date(), SYDNEY_TIMEZONE, 'yyyy-MM-dd');
    
    console.log(`Fetching all race data for Sydney date: ${todayStr}...`);

    const allMeetingsByJur: { [key: string]: any[] } = {};
    const meetingPromises = JURISDICTIONS.map(async (jur) => {
      try {
        const meetings = await getMeetings(todayStr, todayStr, jur);
        allMeetingsByJur[jur] = meetings || [];
      } catch (error: any) {
        console.error(`Failed to get meetings for ${jur}:`, error.message);
        allMeetingsByJur[jur] = [];
      }
    });
    await Promise.all(meetingPromises);

    const allMeetings: any[] = [].concat(...Object.values(allMeetingsByJur) as any);
    const racePromises = allMeetings.map(async (meeting) => {
      try {
        const races = await getRacesForMeeting(meeting.meetingId);
        await new Promise(resolve => setTimeout(resolve, 250));
        return { meetingId: meeting.meetingId, races: races || [] };
      } catch (error) {
        console.error(`Failed to get races for meeting ${meeting.meetingId}`);
        return { meetingId: meeting.meetingId, races: [], error: 'Failed to load races' };
      }
    });
    const raceResults = await Promise.all(racePromises);

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

    db = await openDb();
    
    const [
      dogStats, trainerStats, boxBiasStats, recentPerfStats,
      lastRaceGrades, runningStyleStats, trackSpecificStats, distanceSpecificStats,
      boxPerformanceStats
    ] = await Promise.all([
      getDogStats(db), getTrainerStats(db), getBoxBiasStats(db),
      getRecentPerformanceStats(db), getLastRaceGrades(db),
      getRunningStyleStats(db), getTrackSpecificStats(db), getDistanceSpecificStats(db),
      getBoxPerformanceByDog(db)
    ]);

    const dogStatsMap = new Map<number, { totalStarts: number; winRate: number; placeRate: number }>(dogStats.map((r: any) => [r.dogId, {
      totalStarts: r.totalStarts,
      winRate: r.totalStarts > 0 ? r.wins / r.totalStarts : 0,
      placeRate: r.totalStarts > 0 ? r.places / r.totalStarts : 0
    }]));
    const trainerStatsMap = new Map<number, { trainerStrikeRate: number }>(trainerStats.map((r: any) => [r.trainerId, {
      trainerStrikeRate: r.totalStarts > 0 ? r.wins / r.totalStarts : 0
    }]));
    const boxBiasStatsMap = new Map<string, { boxWinPercentage: number }>(boxBiasStats.map((r: any) => 
      [`${r.trackCode}-${r.boxNumber}`, {
        boxWinPercentage: r.totalStarts > 0 ? r.wins / r.totalStarts : 0
      }]));
    const recentPerfStatsMap = new Map<string, { avgTimeLast5TrackDist: number | null; avgSplitLast5TrackDist: number | null }>(recentPerfStats.map((r: any) => 
      [`${r.dogId}-${r.trackCode}-${r.distanceInMetres}`, {
        avgTimeLast5TrackDist: r.avgTimeLast5TrackDist,
        avgSplitLast5TrackDist: r.avgSplitLast5TrackDist
      }]));
    const lastRaceGradeMap = new Map<number, string | null>(lastRaceGrades.map((r: any) => [r.dogId, r.lastRaceOutgoingGrade]));
    const runningStyleMap = new Map<number, { avgFirstSplitPosition: number | null; avgFirstSplitPositionL5: number | null; leadAtFirstBendRate: number | null }>(runningStyleStats.map((r: any) => [r.dogId, {
      avgFirstSplitPosition: r.avgFirstSplitPosition,
      avgFirstSplitPositionL5: r.avgFirstSplitPositionL5,
      leadAtFirstBendRate: r.leadAtFirstBendRate
    }]));
    
    const trackSpecificMap = new Map<string, { startsAtTrack: number; winRateAtTrack: number; placeRateAtTrack: number }>();
    trackSpecificStats.forEach((r: any) => {
      const key = `${r.dogId}-${r.trackCode}`;
      trackSpecificMap.set(key, {
        startsAtTrack: r.startsAtTrack,
        winRateAtTrack: r.winRateAtTrack,
        placeRateAtTrack: r.placeRateAtTrack
      });
    });

    const distanceSpecificMap = new Map<string, { startsAtDistance: number; winRateAtDistance: number; placeRateAtDistance: number }>();
    distanceSpecificStats.forEach((r: any) => {
      const key = `${r.dogId}-${r.distanceInMetres}`;
      distanceSpecificMap.set(key, {
        startsAtDistance: r.startsAtDistance,
        winRateAtDistance: r.winRateAtDistance,
        placeRateAtDistance: r.placeRateAtDistance
      });
    });

    const boxPerformanceMap = new Map<number, { boxGroup: string; starts: number; winRate: number; avgTime: number }[]>();
    boxPerformanceStats.forEach((r: any) => {
      if (!boxPerformanceMap.has(r.dogId)) {
        boxPerformanceMap.set(r.dogId, []);
      }
      boxPerformanceMap.get(r.dogId)!.push({
        boxGroup: r.boxGroup,
        starts: r.starts,
        winRate: r.winRate,
        avgTime: r.avgTime
      });
    });

    for (const jur in allData) {
      for (const meeting of allData[jur]!) {
        if (meeting.races) {
          for (const race of meeting.races) {
            if (race.runs) {
              race.runs = race.runs.map((run: any) => {
                const { dogId, trainerId, boxNumber } = run;
                const { trackCode, distance } = { trackCode: meeting.trackCode, distance: race.distance };

                const dogStat = dogStatsMap.get(dogId);
                const trainerStat = trainerStatsMap.get(trainerId);
                const boxStat = boxBiasStatsMap.get(`${trackCode}-${boxNumber}`);
                const recentStat = recentPerfStatsMap.get(`${dogId}-${trackCode}-${distance}`);
                const lastGrade = lastRaceGradeMap.get(dogId) ?? null;
                const runningStyle = runningStyleMap.get(dogId);
                const trackSpecific = trackSpecificMap.get(`${dogId}-${trackCode}`);
                const distanceSpecific = distanceSpecificMap.get(`${dogId}-${distance}`);
                const boxPreferenceData = boxPerformanceMap.get(dogId) ?? null;

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

    res.json(allData);
  } catch (error: any) {
    console.error('Fatal error in /api/races/today/all:', error.message, error.stack);
    res.status(500).json({ error: 'Failed to fetch all race data' });
  } finally {
    if (db) {
      await db.close();
    }
  }
});

app.get('/api/daily-races', async (req, res) => {
  try {
    const result = await getLatestDailyRaces();
    
    if (!result) {
      return res.status(404).json({ 
        error: 'No pre-computed data found',
        message: 'Run `npm run daily-compute` first'
      });
    }
    
    res.json({
      date: result.race_date,
      computedAt: result.computed_at,
      data: result.data
    });
  } catch (error: any) {
    console.error('Error fetching daily races:', error.message);
    res.status(500).json({ error: 'Failed to fetch daily races' });
  }
});

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
