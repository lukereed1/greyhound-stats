import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';

import { getLatestDailyRaces } from './supabase.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

app.use(cors({
    origin: '*', 
    methods: ['GET', 'POST']
}));

const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    message: 'Greyhound Stats API is running',
    timestamp: new Date().toISOString(),
    endpoints: {
      races: '/api/daily-races'
    }
  });
});

app.get('/api/daily-races', async (req, res) => {
  try {
    const result = await getLatestDailyRaces();
    
    if (!result) {
      return res.status(404).json({ 
        error: 'No pre-computed data found',
        message: 'Data is generated via scheduled task'
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

// Listen on 0.0.0.0 for Fly.io
app.listen(Number(PORT), '0.0.0.0', () => {
  console.log(`Server running at http://0.0.0.0:${PORT}`);
});