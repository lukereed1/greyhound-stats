import { createClient } from '@supabase/supabase-js';
import * as dotenv from 'dotenv';

dotenv.config();

const supabaseUrl = process.env.SUPABASE_URL!;
const supabaseKey = process.env.SUPABASE_KEY!;

if (!supabaseUrl || !supabaseKey) {
  throw new Error('Missing SUPABASE_URL or SUPABASE_KEY in environment variables');
}

export const supabase = createClient(supabaseUrl, supabaseKey);

export const saveDailyRaces = async (raceDate: string, data: any) => {
  const { error } = await supabase
    .from('daily_races')
    .upsert({
      race_date: raceDate,
      data: data
    }, {
      onConflict: 'race_date'
    });
  
  if (error) throw error;
};

export const getLatestDailyRaces = async () => {
  const { data, error } = await supabase
    .from('daily_races')
    .select('race_date, data, computed_at')
    .order('race_date', { ascending: false })
    .limit(1)
    .single();
  
  if (error && error.code !== 'PGRST116') throw error;
  return data || null;
};
