// src/server.ts
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { formatInTimeZone } from 'date-fns-tz';
import { getMeetings, getRacesForMeeting } from './api.js';
import {
  openDb,
  getDogStats, getTrainerStats, getBoxBiasStats,
  getRecentPerformanceStats, getLastRaceGrades, getPerformanceRatings,
  getCareerPrizeMoney, getConsistencyScores, getEarlySpeedRatings,
  getRunningStyleStats, getTrackSpecificStats, getDistanceSpecificStats,
  getBoxPerformanceByDog, getWeightedRecentForm
} from './db.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = 3000;

const JURISDICTIONS = ['VIC', 'NSW', 'QLD', 'SA', 'WA', 'TAS', 'ACT', 'NT', 'NZ'];

app.use(express.static(path.join(__dirname, 'public')));

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
    console.log("Finished fetching all meetings.");

    const allMeetings = [].concat(...Object.values(allMeetingsByJur));
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
    console.log("Finished fetching all races.");

    const racesByMeetingId = new Map();
    raceResults.forEach(result => {
      racesByMeetingId.set(result.meetingId, result);
    });

    const allData: { [key: string]: any[] } = {};
    for (const jur in allMeetingsByJur) {
      allData[jur] = allMeetingsByJur[jur].map(meeting => {
        const raceResult = racesByMeetingId.get(meeting.meetingId);
        return {
          ...meeting,
          races: raceResult ? raceResult.races : [],
          error: raceResult ? raceResult.error : undefined
        };
      });
    }

    db = await openDb();
    console.log("Fetching all statistics from DB...");
    
    const [
      dogStats, trainerStats, boxBiasStats, recentPerfStats,
      lastRaceGrades, performanceRatings, prizeMoney, 
      consistencyScores, earlySpeedRatings,
      runningStyleStats, trackSpecificStats, distanceSpecificStats,
      boxPerformanceStats, weightedRecentForm
    ] = await Promise.all([
      getDogStats(db), getTrainerStats(db), getBoxBiasStats(db),
      getRecentPerformanceStats(db), getLastRaceGrades(db), getPerformanceRatings(db),
      getCareerPrizeMoney(db), getConsistencyScores(db), getEarlySpeedRatings(db),
      getRunningStyleStats(db), getTrackSpecificStats(db), getDistanceSpecificStats(db),
      getBoxPerformanceByDog(db), getWeightedRecentForm(db)
    ]);

    const dogStatsMap = new Map(dogStats.map(r => [r.dogId, {
      totalStarts: r.totalStarts,
      winRate: r.totalStarts > 0 ? r.wins / r.totalStarts : 0,
      placeRate: r.totalStarts > 0 ? r.places / r.totalStarts : 0
    }]));
    const trainerStatsMap = new Map(trainerStats.map(r => [r.trainerId, {
      trainerStrikeRate: r.totalStarts > 0 ? r.wins / r.totalStarts : 0
    }]));
    const boxBiasStatsMap = new Map(boxBiasStats.map(r => 
      [`${r.trackCode}-${r.boxNumber}`, {
        boxWinPercentage: r.totalStarts > 0 ? r.wins / r.totalStarts : 0
      }]));
    const recentPerfStatsMap = new Map(recentPerfStats.map(r => 
      [`${r.dogId}-${r.trackCode}-${r.distanceInMetres}`, {
        avgTimeLast5TrackDist: r.avgTimeLast5TrackDist,
        avgSplitLast5TrackDist: r.avgSplitLast5TrackDist
      }]));
    const lastRaceGradeMap = new Map(lastRaceGrades.map(r => [r.dogId, r.lastRaceOutgoingGrade]));
    const performanceRatingsMap = new Map(performanceRatings.map(r => [r.dogId, {
      careerPerformanceScore: r.careerPerformanceScore,
      last5PerformanceScore: r.last5PerformanceScore
    }]));
    const prizeMoneyMap = new Map(prizeMoney.map(r => [r.dogId, r.careerPrizeMoney]));
    const consistencyMap = new Map(consistencyScores.map(r => [r.dogId, r.consistencyScore]));
    const earlySpeedMap = new Map(earlySpeedRatings.map(r => [r.dogId, r.last5EarlySpeedRating]));
    const runningStyleMap = new Map(runningStyleStats.map(r => [r.dogId, {
      avgFirstSplitPosition: r.avgFirstSplitPosition,
      avgFirstSplitPositionL5: r.avgFirstSplitPositionL5,
      leadAtFirstBendRate: r.leadAtFirstBendRate
    }]));
    
    const trackSpecificMap = new Map();
    trackSpecificStats.forEach(r => {
      const key = `${r.dogId}-${r.trackCode}`;
      trackSpecificMap.set(key, {
        startsAtTrack: r.startsAtTrack,
        winRateAtTrack: r.winRateAtTrack,
        placeRateAtTrack: r.placeRateAtTrack
      });
    });

    const distanceSpecificMap = new Map();
    distanceSpecificStats.forEach(r => {
      const key = `${r.dogId}-${r.distanceInMetres}`;
      distanceSpecificMap.set(key, {
        startsAtDistance: r.startsAtDistance,
        winRateAtDistance: r.winRateAtDistance,
        placeRateAtDistance: r.placeRateAtDistance
      });
    });

    const boxPerformanceMap = new Map();
    boxPerformanceStats.forEach(r => {
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

    const weightedFormMap = new Map(weightedRecentForm.map(r => [r.dogId, {
      weightedAvgPlace: r.weightedAvgPlace,
      recentImprovement: r.recentImprovement
    }]));

    console.log("Finished creating lookup maps.");

    for (const jur in allData) {
      for (const meeting of allData[jur]) {
        if (meeting.races) {
          for (const race of meeting.races) {
            if (race.runs) {
              race.runs = race.runs.map(run => {
                const { dogId, trainerId, boxNumber } = run;
                const { trackCode, distance } = { trackCode: meeting.trackCode, distance: race.distance };

                const dogStat = dogStatsMap.get(dogId);
                const trainerStat = trainerStatsMap.get(trainerId);
                const boxStat = boxBiasStatsMap.get(`${trackCode}-${boxNumber}`);
                const recentStat = recentPerfStatsMap.get(`${dogId}-${trackCode}-${distance}`);
                const lastGrade = lastRaceGradeMap.get(dogId);
                const perfRating = performanceRatingsMap.get(dogId);
                const runningStyle = runningStyleMap.get(dogId);
                const trackSpecific = trackSpecificMap.get(`${dogId}-${trackCode}`);
                const distanceSpecific = distanceSpecificMap.get(`${dogId}-${distance}`);
                const boxPreferenceData = boxPerformanceMap.get(dogId);
                const weightedForm = weightedFormMap.get(dogId);

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
                  careerPerformanceScore: perfRating?.careerPerformanceScore ?? null,
                  last5PerformanceScore: perfRating?.last5PerformanceScore ?? null,
                  careerPrizeMoney: prizeMoneyMap.get(dogId) || null,
                  consistencyScore: consistencyMap.get(dogId) || null,
                  last5EarlySpeedRating: earlySpeedMap.get(dogId) || null,
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
                  boxPreferenceData: boxPreferenceData || null,
                  weightedAvgPlace: weightedForm?.weightedAvgPlace ?? null,
                  recentImprovement: weightedForm?.recentImprovement ?? null,
                };
              });
            }
          }
        }
      }
    }

    console.log("Finished fetching and processing all data.");
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

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});