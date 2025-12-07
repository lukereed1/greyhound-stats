import Database from 'better-sqlite3';
import type { Database as DatabaseType } from 'better-sqlite3';
import path from 'path';

let db: DatabaseType | null = null;
const DB_PATH = path.join(process.cwd(), 'runs.db');

export function getDb(): DatabaseType {
  if (!db) {
    console.log(`[DB] Opening database at: ${DB_PATH}`);
    db = new Database(DB_PATH);
    db.pragma('journal_mode = WAL');
  }
  return db;
}

export async function openDb(): Promise<DatabaseType> {
  return getDb();
}

export function getOne(db: DatabaseType, sql: string): any {
  return db.prepare(sql).get();
}

export const getDogStats = async (db: DatabaseType) => {
  const start = Date.now();
  const result = db.prepare(`
    SELECT dogId,
      COUNT(*) as totalStarts,
      SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) as wins,
      SUM(CASE WHEN place IN (1, 2, 3) THEN 1 ELSE 0 END) as places
    FROM runs
    WHERE scratched = 0
    GROUP BY dogId
  `).all();
  console.log(`[DB] getDogStats: ${((Date.now() - start) / 1000).toFixed(1)}s, ${result.length} dogs`);
  return result;
};

export const getTrainerStats = async (db: DatabaseType) => {
  const start = Date.now();
  const result = db.prepare(`
    SELECT trainerId,
      COUNT(*) as totalStarts,
      SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) as wins
    FROM runs
    WHERE scratched = 0 AND trainerId IS NOT NULL
    GROUP BY trainerId
  `).all();
  console.log(`[DB] getTrainerStats: ${((Date.now() - start) / 1000).toFixed(1)}s, ${result.length} trainers`);
  return result;
};

export const getBoxBiasStats = async (db: DatabaseType) => {
  const start = Date.now();
  const result = db.prepare(`
    SELECT trackCode, boxNumber,
      COUNT(*) as totalStarts,
      SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) as wins
    FROM runs
    WHERE scratched = 0
      AND trackCode IS NOT NULL
      AND boxNumber > 0
    GROUP BY trackCode, boxNumber
  `).all();
  console.log(`[DB] getBoxBiasStats: ${((Date.now() - start) / 1000).toFixed(1)}s`);
  return result;
};

export const getRecentPerformanceStats = async (db: DatabaseType) => {
  const start = Date.now();
  
  const timeStats = db.prepare(`
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
  `).all();

  const splitStats = db.prepare(`
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
  `).all();

  const mergedMap = new Map();
  
  (timeStats as any[]).forEach((row: any) => {
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

  (splitStats as any[]).forEach((row: any) => {
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

  console.log(`[DB] getRecentPerformanceStats: ${((Date.now() - start) / 1000).toFixed(1)}s`);
  return Array.from(mergedMap.values());
};

export const getLastRaceGrades = async (db: DatabaseType) => {
  const start = Date.now();
  const result = db.prepare(`
    WITH LastRun AS (
      SELECT dogId, outgoingGrade,
        ROW_NUMBER() OVER(PARTITION BY dogId ORDER BY meetingDate DESC, raceNumber DESC) as rn
      FROM runs
      WHERE (scratched = 0 OR scratched IS NULL) AND outgoingGrade IS NOT NULL
    )
    SELECT dogId, outgoingGrade as lastRaceOutgoingGrade
    FROM LastRun WHERE rn = 1
  `).all();
  console.log(`[DB] getLastRaceGrades: ${((Date.now() - start) / 1000).toFixed(1)}s`);
  return result;
};

export const getRunningStyleStats = async (db: DatabaseType) => {
  const start = Date.now();
  const result = db.prepare(`
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
  `).all();
  console.log(`[DB] getRunningStyleStats: ${((Date.now() - start) / 1000).toFixed(1)}s`);
  return result;
};

export const getTrackSpecificStats = async (db: DatabaseType) => {
  const start = Date.now();
  const result = db.prepare(`
    SELECT dogId, trackCode,
      COUNT(*) as startsAtTrack,
      SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as winRateAtTrack,
      SUM(CASE WHEN place <= 3 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as placeRateAtTrack
    FROM runs
    WHERE (scratched = 0 OR scratched IS NULL)
    GROUP BY dogId, trackCode
    HAVING COUNT(*) >= 3
  `).all();
  console.log(`[DB] getTrackSpecificStats: ${((Date.now() - start) / 1000).toFixed(1)}s`);
  return result;
};

export const getDistanceSpecificStats = async (db: DatabaseType) => {
  const start = Date.now();
  const result = db.prepare(`
    SELECT dogId, distanceInMetres,
      COUNT(*) as startsAtDistance,
      SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as winRateAtDistance,
      SUM(CASE WHEN place <= 3 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as placeRateAtDistance,
      AVG(resultTime) as avgTimeAtDistance
    FROM runs
    WHERE (scratched = 0 OR scratched IS NULL) AND resultTime > 0
    GROUP BY dogId, distanceInMetres
    HAVING COUNT(*) >= 2
  `).all();
  console.log(`[DB] getDistanceSpecificStats: ${((Date.now() - start) / 1000).toFixed(1)}s`);
  return result;
};

export const getBoxPerformanceByDog = async (db: DatabaseType) => {
  const start = Date.now();
  const result = db.prepare(`
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
  `).all();
  console.log(`[DB] getBoxPerformanceByDog: ${((Date.now() - start) / 1000).toFixed(1)}s`);
  return result;
};

export const createTable = async (): Promise<DatabaseType> => {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS runs (
      runId INTEGER PRIMARY KEY, 
      trackCode TEXT, trackName TEXT, distanceInMetres INTEGER,
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
      firstSplitTime REAL, secondSplitPosition INTEGER, secondSplitTime REAL,
      bestTimeTrackDistance REAL
    )
  `);
  
  db.exec(`CREATE INDEX IF NOT EXISTS idx_dog_track_dist ON runs (dogId, trackCode, distanceInMetres)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_trainer ON runs (trainerId)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_track_box ON runs (trackCode, boxNumber)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_dog_date ON runs (dogId, meetingDate DESC)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_dog_track_split ON runs (dogId, trackCode)`);
  
  return db;
};

export const insertRun = async (db: Database.Database, run: any) => {
  const columns = [
    'runId', 'trackCode', 'trackName', 'distanceInMetres', 'raceId', 'meetingDate', 'raceTypeCode', 'raceType',
    'dogId', 'dogName', 'weightInKg', 'incomingGrade', 'outgoingGrade', 'gradedTo', 'rating', 'raceNumber',
    'boxNumber', 'boxDrawnOrder', 'rugNumber', 'startPrice', 'place', 'abnormalResult', 'scratched',
    'prizeMoney', 'resultTime', 'resultMargin', 'resultMarginLengths', 'startPaceCode', 'jumpCode',
    'runLineCode', 'colourCode', 'sex', 'comment', 'ownerId', 'trainerId', 'ownerName', 'ownerState',
    'trainerName', 'trainerSuburb', 'trainerState', 'trainerPostCode', 'trainerDistrict', 'isQuad',
    'isBestBet', 'damId', 'damName', 'sireId', 'sireName', 'dateWhelped', 'isLateScratching', 'last5',
    'firstSecond', 'pir', 'careerPrizeMoney', 'averageSpeed', 'unplaced', 'unplacedCode', 'totalFormCount',
    'bestTime', 'firstSplitPosition', 'firstSplitTime', 'secondSplitPosition', 'secondSplitTime', 'bestTimeTrackDistance'
  ];
  
  const placeholders = columns.map(() => '?').join(',');
  const values = columns.map(col => {
    const val = run[col];
    return typeof val === 'boolean' ? (val ? 1 : 0) : val;
  });
  
  const stmt = db.prepare(`INSERT OR REPLACE INTO runs (${columns.join(',')}) VALUES (${placeholders})`);
  stmt.run(...values);
};
