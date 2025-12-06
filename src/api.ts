// src/api.ts
import axios from 'axios';
import dotenv from 'dotenv';

dotenv.config();

const BASE_URL = process.env.BASE_URL;
const API_KEY = process.env.TOPAZ_API_KEY;

if (!BASE_URL || !API_KEY) {
  console.error("Error: BASE_URL or TOPAZ_API_KEY is not defined in .env file.");
  process.exit(1);
}

const apiClient = axios.create({
  baseURL: BASE_URL,
  headers: { 'accept': 'application/json', 'X-API-Key': API_KEY },
  timeout: 10000
});

// THIS FUNCTION IS FOR THE YEARLY SCRAPER
export const getBulkRuns = async (jurisdiction: string, year: number, month: number) => {
  try {
    const response = await apiClient.get(`/bulk/runs/${jurisdiction}/${year}/${month}`);
    return response.data;
  } catch (error) {
    console.error("Error fetching bulk runs:", error.message);
    throw error;
  }
};

// THIS FUNCTION IS FOR THE MONTHLY SCRAPER
export const getBulkRunsByDay = async (jurisdiction: string, year: number, month: number, day: number) => {
  try {
    const response = await apiClient.get(`/bulk/runs/${jurisdiction}/${year}/${month}/${day}`);
    return response.data;
  } catch (error) {
    console.error("Error fetching bulk runs by day:", error.message);
    throw error;
  }
};

export const getMeetings = async (fromDate: string, toDate?: string, jurisdiction?: string) => {
  try {
    const params = new URLSearchParams({ from: fromDate });
    if (toDate) params.append('to', toDate);
    if (jurisdiction) params.append('owningauthoritycode', jurisdiction);
    const response = await apiClient.get('/meeting', { params });
    return response.data;
  } catch (error) {
    console.error("Error fetching meetings:", error.message);
    throw error;
  }
};

export const getRacesForMeeting = async (meetingId: number) => {
  try {
    const response = await apiClient.get(`/meeting/${meetingId}/races`);
    return response.data;
  } catch (error) {
    console.error(`Error fetching races for meeting ${meetingId}:`, error.message);
    throw error;
  }
};