// src/db.ts
import sqlite3 from 'sqlite3';
import { open, Database } from 'sqlite';

export async function openDb() {
  return open({
    filename: './runs.db',
    driver: sqlite3.Database
  });
}

// === QUERIES ===
export const getDogStats = async (db: Database) => {
  return db.all(`
    SELECT dogId,
      COUNT(*) as totalStarts,
      SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) as wins,
      SUM(CASE WHEN place IN (1, 2, 3) THEN 1 ELSE 0 END) as places
    FROM runs
    WHERE scratched = 0 OR scratched IS NULL
    GROUP BY dogId
  `);
};

export const getTrainerStats = async (db: Database) => {
  return db.all(`
    SELECT trainerId,
      COUNT(*) as totalStarts,
      SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) as wins
    FROM runs
    WHERE (scratched = 0 OR scratched IS NULL) AND trainerId IS NOT NULL
    GROUP BY trainerId
  `);
};

export const getBoxBiasStats = async (db: Database) => {
  return db.all(`
    SELECT trackCode, boxNumber,
      COUNT(*) as totalStarts,
      SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) as wins
    FROM runs
    WHERE (scratched = 0 OR scratched IS NULL)
      AND trackCode IS NOT NULL
      AND boxNumber IS NOT NULL
      AND boxNumber > 0
    GROUP BY trackCode, boxNumber
  `);
};

// UPDATED QUERY: Split into two separate queries to handle times and splits independently
export const getRecentPerformanceStats = async (db: Database) => {
  // Get average times (doesn't require split times)
  const timeStats = await db.all(`
    WITH RankedRuns AS (
      SELECT dogId, trackCode, distanceInMetres, resultTime,
        ROW_NUMBER() OVER(PARTITION BY dogId, trackCode, distanceInMetres ORDER BY meetingDate DESC) as rn
      FROM runs
      WHERE (scratched = 0 OR scratched IS NULL) AND resultTime > 0
    )
    SELECT dogId, trackCode, distanceInMetres,
      AVG(resultTime) as avgTimeLast5TrackDist,
      COUNT(*) as runsAtTrackDist
    FROM RankedRuns
    WHERE rn <= 5
    GROUP BY dogId, trackCode, distanceInMetres
    HAVING COUNT(*) >= 1
  `);

  // Get average splits (separate query for split times)
  const splitStats = await db.all(`
    WITH RankedRuns AS (
      SELECT dogId, trackCode, distanceInMetres, firstSplitTime,
        ROW_NUMBER() OVER(PARTITION BY dogId, trackCode, distanceInMetres ORDER BY meetingDate DESC) as rn
      FROM runs
      WHERE (scratched = 0 OR scratched IS NULL) AND firstSplitTime > 0
    )
    SELECT dogId, trackCode, distanceInMetres,
      AVG(firstSplitTime) as avgSplitLast5TrackDist
    FROM RankedRuns
    WHERE rn <= 5
    GROUP BY dogId, trackCode, distanceInMetres
    HAVING COUNT(*) >= 1
  `);

  // Merge the results
  const mergedMap = new Map();
  
  timeStats.forEach(row => {
    const key = `${row.dogId}-${row.trackCode}-${row.distanceInMetres}`;
    mergedMap.set(key, {
      dogId: row.dogId,
      trackCode: row.trackCode,
      distanceInMetres: row.distanceInMetres,
      avgTimeLast5TrackDist: row.avgTimeLast5TrackDist,
      runsAtTrackDist: row.runsAtTrackDist,
      avgSplitLast5TrackDist: null
    });
  });

  splitStats.forEach(row => {
    const key = `${row.dogId}-${row.trackCode}-${row.distanceInMetres}`;
    if (mergedMap.has(key)) {
      mergedMap.get(key).avgSplitLast5TrackDist = row.avgSplitLast5TrackDist;
    } else {
      mergedMap.set(key, {
        dogId: row.dogId,
        trackCode: row.trackCode,
        distanceInMetres: row.distanceInMetres,
        avgTimeLast5TrackDist: null,
        runsAtTrackDist: 0,
        avgSplitLast5TrackDist: row.avgSplitLast5TrackDist
      });
    }
  });

  return Array.from(mergedMap.values());
};

export const getLastRaceGrades = async (db: Database) => {
  return db.all(`
    WITH LastRun AS (
      SELECT dogId, outgoingGrade,
        ROW_NUMBER() OVER(PARTITION BY dogId ORDER BY meetingDate DESC, raceNumber DESC) as rn
      FROM runs
      WHERE (scratched = 0 OR scratched IS NULL) AND outgoingGrade IS NOT NULL
    )
    SELECT dogId, outgoingGrade as lastRaceOutgoingGrade
    FROM LastRun WHERE rn = 1
  `);
};

export const getPerformanceRatings = async (db: Database) => {
  return db.all(`
    WITH Benchmarks AS (
      SELECT trackCode, distanceInMetres, MIN(resultTime) as benchmarkTime
      FROM runs
      WHERE resultTime > 0 AND (scratched = 0 OR scratched IS NULL)
      GROUP BY trackCode, distanceInMetres
    ),
    RunScores AS (
      SELECT r.dogId,
        (b.benchmarkTime / r.resultTime) * 100 as performanceScore,
        ROW_NUMBER() OVER(PARTITION BY r.dogId ORDER BY r.meetingDate DESC, r.raceNumber DESC) as rn
      FROM runs r
      JOIN Benchmarks b ON r.trackCode = b.trackCode AND r.distanceInMetres = b.distanceInMetres
      WHERE r.resultTime > 0 AND (r.scratched = 0 OR r.scratched IS NULL)
    )
    SELECT dogId,
      AVG(performanceScore) as careerPerformanceScore,
      AVG(CASE WHEN rn <= 5 THEN performanceScore ELSE NULL END) as last5PerformanceScore
    FROM RunScores
    GROUP BY dogId
  `);
};

export const getCareerPrizeMoney = async (db: Database) => {
  return db.all(`
    WITH LatestRun AS (
      SELECT dogId, careerPrizeMoney,
        ROW_NUMBER() OVER(PARTITION BY dogId ORDER BY meetingDate DESC, raceNumber DESC) as rn
      FROM runs WHERE careerPrizeMoney IS NOT NULL AND careerPrizeMoney > 0
    )
    SELECT dogId, careerPrizeMoney FROM LatestRun WHERE rn = 1
  `);
};

export const getConsistencyScores = async (db: Database) => {
  return db.all(`
    WITH DogStats AS (
      SELECT dogId,
        AVG(resultTime) as avgTime,
        SQRT((SUM(POWER(resultTime, 2)) - POWER(SUM(resultTime), 2) / COUNT(resultTime)) / (COUNT(resultTime) - 1)) as stdevTime
      FROM runs
      WHERE resultTime > 0 AND (scratched = 0 OR scratched IS NULL)
      GROUP BY dogId
      HAVING COUNT(resultTime) > 4
    )
    SELECT dogId, (1 - (stdevTime / avgTime)) * 100 as consistencyScore
    FROM DogStats WHERE avgTime > 0
  `);
};

export const getEarlySpeedRatings = async (db: Database) => {
  return db.all(`
    WITH SplitBenchmarks AS (
      SELECT trackCode, MIN(firstSplitTime) as benchmarkSplit
      FROM runs
      WHERE firstSplitTime > 0 AND (scratched = 0 OR scratched IS NULL)
      GROUP BY trackCode
    ),
    SplitRunScores AS (
      SELECT r.dogId,
        (b.benchmarkSplit / r.firstSplitTime) * 100 as earlySpeedScore,
        ROW_NUMBER() OVER(PARTITION BY r.dogId ORDER BY r.meetingDate DESC, r.raceNumber DESC) as rn
      FROM runs r
      JOIN SplitBenchmarks b ON r.trackCode = b.trackCode
      WHERE r.firstSplitTime > 0 AND (r.scratched = 0 OR r.scratched IS NULL)
    )
    SELECT dogId,
      AVG(CASE WHEN rn <= 5 THEN earlySpeedScore ELSE NULL END) as last5EarlySpeedRating
    FROM SplitRunScores
    GROUP BY dogId
  `);
};

// === NEW ADVANCED QUERIES ===

export const getRunningStyleStats = async (db: Database) => {
  return db.all(`
    WITH RecentRuns AS (
      SELECT dogId, firstSplitPosition, place,
        ROW_NUMBER() OVER(PARTITION BY dogId ORDER BY meetingDate DESC) as rn
      FROM runs
      WHERE (scratched = 0 OR scratched IS NULL)
        AND firstSplitPosition IS NOT NULL
        AND firstSplitPosition > 0
    )
    SELECT dogId,
      AVG(firstSplitPosition) as avgFirstSplitPosition,
      AVG(CASE WHEN rn <= 5 THEN firstSplitPosition END) as avgFirstSplitPositionL5,
      SUM(CASE WHEN firstSplitPosition = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as leadAtFirstBendRate
    FROM RecentRuns
    WHERE rn <= 10
    GROUP BY dogId
  `);
};

export const getTrackSpecificStats = async (db: Database) => {
  return db.all(`
    SELECT dogId, trackCode,
      COUNT(*) as startsAtTrack,
      SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as winRateAtTrack,
      SUM(CASE WHEN place <= 3 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as placeRateAtTrack
    FROM runs
    WHERE (scratched = 0 OR scratched IS NULL)
    GROUP BY dogId, trackCode
    HAVING COUNT(*) >= 3
  `);
};

export const getDistanceSpecificStats = async (db: Database) => {
  return db.all(`
    SELECT dogId, distanceInMetres,
      COUNT(*) as startsAtDistance,
      SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as winRateAtDistance,
      SUM(CASE WHEN place <= 3 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as placeRateAtDistance,
      AVG(resultTime) as avgTimeAtDistance
    FROM runs
    WHERE (scratched = 0 OR scratched IS NULL) AND resultTime > 0
    GROUP BY dogId, distanceInMetres
    HAVING COUNT(*) >= 2
  `);
};

export const getBoxPerformanceByDog = async (db: Database) => {
  return db.all(`
    SELECT dogId,
      CASE
        WHEN boxNumber IN (1, 2) THEN 'inside'
        WHEN boxNumber IN (3, 4, 5) THEN 'middle'
        ELSE 'outside'
      END as boxGroup,
      COUNT(*) as starts,
      SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as winRate,
      AVG(resultTime) as avgTime
    FROM runs
    WHERE (scratched = 0 OR scratched IS NULL)
      AND boxNumber > 0
      AND resultTime > 0
    GROUP BY dogId, boxGroup
    HAVING COUNT(*) >= 2
  `);
};

export const getWeightedRecentForm = async (db: Database) => {
  return db.all(`
    WITH RankedRuns AS (
      SELECT dogId, place, resultTime, meetingDate,
        ROW_NUMBER() OVER(PARTITION BY dogId ORDER BY meetingDate DESC) as rn
      FROM runs
      WHERE (scratched = 0 OR scratched IS NULL) AND resultTime > 0
    )
    SELECT dogId,
      (SUM(CASE WHEN rn = 1 THEN place * 5
               WHEN rn = 2 THEN place * 4
               WHEN rn = 3 THEN place * 3
               WHEN rn = 4 THEN place * 2
               WHEN rn = 5 THEN place * 1
               ELSE 0 END) * 1.0) / 15 as weightedAvgPlace,
      AVG(CASE WHEN rn <= 3 THEN place END) - AVG(CASE WHEN rn BETWEEN 4 AND 6 THEN place END) as recentImprovement
    FROM RankedRuns
    WHERE rn <= 6
    GROUP BY dogId
  `);
};

// === TABLE AND INSERT LOGIC ===
export const createTable = async () => {
  const db = await openDb();
  await db.exec(`
    CREATE TABLE IF NOT EXISTS runs (
      runId INTEGER PRIMARY KEY, trackCode TEXT, trackName TEXT, distanceInMetres INTEGER,
      raceId INTEGER, meetingDate TEXT, raceTypeCode TEXT, raceType TEXT, dogId INTEGER,
      dogName TEXT, weightInKg REAL, incomingGrade TEXT, outgoingGrade TEXT, gradedTo TEXT,
      rating INTEGER, raceNumber INTEGER, boxNumber INTEGER, boxDrawnOrder INTEGER,
      rugNumber INTEGER, startPrice REAL, place INTEGER, abnormalResult TEXT, scratched INTEGER,
      prizeMoney REAL, resultTime REAL, resultMargin REAL, resultMarginLengths TEXT,
      startPaceCode TEXT, jumpCode TEXT, runLineCode TEXT, colourCode TEXT, sex TEXT,
      comment TEXT, ownerId INTEGER, trainerId INTEGER, ownerName TEXT, ownerState TEXT,
      trainerName TEXT, trainerSuburb TEXT, trainerState TEXT, trainerPostCode TEXT,
      trainerDistrict TEXT, isQuad INTEGER, isBestBet INTEGER, damId INTEGER, damName TEXT,
      sireId INTEGER, sireName TEXT, dateWhelped TEXT, isLateScratching INTEGER, last5 TEXT,
      firstSecond TEXT, pir TEXT, careerPrizeMoney REAL, averageSpeed REAL, unplaced TEXT,
      unplacedCode TEXT, totalFormCount INTEGER, bestTime TEXT, firstSplitPosition INTEGER,
      firstSplitTime REAL, secondSplitTime REAL,
      bestTimeTrackDistance REAL
    )
  `);
  await db.exec(`CREATE INDEX IF NOT EXISTS idx_dog_track_dist ON runs (dogId, trackCode, distanceInMetres)`);
  await db.exec(`CREATE INDEX IF NOT EXISTS idx_trainer ON runs (trainerId)`);
  await db.exec(`CREATE INDEX IF NOT EXISTS idx_track_box ON runs (trackCode, boxNumber)`);
  await db.exec(`CREATE INDEX IF NOT EXISTS idx_dog_date ON runs (dogId, meetingDate DESC)`);
  await db.exec(`CREATE INDEX IF NOT EXISTS idx_dog_track_split ON runs (dogId, trackCode)`);
  return db;
}

export const insertRun = async (db: Database, run: any) => {
  const columns = [
    'runId', 'trackCode', 'trackName', 'distanceInMetres', 'raceId', 'meetingDate', 'raceTypeCode', 'raceType',
    'dogId', 'dogName', 'weightInKg', 'incomingGrade', 'outgoingGrade', 'gradedTo', 'rating', 'raceNumber',
    'boxNumber', 'boxDrawnOrder', 'rugNumber', 'startPrice', 'place', 'abnormalResult', 'scratched',
    'prizeMoney', 'resultTime', 'resultMargin', 'resultMarginLengths', 'startPaceCode', 'jumpCode',
    'runLineCode', 'colourCode', 'sex', 'comment', 'ownerId', 'trainerId', 'ownerName', 'ownerState',
    'trainerName', 'trainerSuburb', 'trainerState', 'trainerPostCode', 'trainerDistrict', 'isQuad',
    'isBestBet', 'damId', 'damName', 'sireId', 'sireName', 'dateWhelped', 'isLateScratching', 'last5',
    'firstSecond', 'pir', 'careerPrizeMoney', 'averageSpeed', 'unplaced', 'unplacedCode', 'totalFormCount',
    'bestTime', 'firstSplitPosition', 'firstSplitTime', 'secondSplitTime', 'bestTimeTrackDistance'
  ];
  const values = columns.map(col => {
    const val = run[col];
    return typeof val === 'boolean' ? (val ? 1 : 0) : val;
  });
  const placeholders = columns.map(() => '?').join(',');
  await db.run(
    `INSERT OR REPLACE INTO runs (${columns.join(',')}) VALUES (${placeholders})`,
    values
  );
};