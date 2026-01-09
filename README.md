# Greyhound Stats

[Demo](https://greyhound-stats.fly.dev/)

- Daily races are fetched locally every morning
- Once daily races fetched, stats for each runner are computed based on their historical data which is stored in a local sqlite db
- Results scraped every month to update local db
- Uses HTML, CSS, TypeScript
- Hosted on fly.io free tier (can be slow or sometimes crash)
