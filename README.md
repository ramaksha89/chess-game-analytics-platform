# ♟️ Chess Game Analytics Platform

> A personal data engineering project combining my passion for chess with modern data stack technologies

## Why This Project?

I was a serious tournament chess player for years, and while I didn't play much online, I spent countless hours analyzing games with ChessBase and Fritz, watching master games, and solving tactical puzzles on Chess.com. As a data engineer working with Databricks daily, I thought - why not combine my chess background with my technical skills to build something that analyzes the game data in ways traditional chess software doesn't?
This project ingests game data from Chess.com and Lichess APIs, processes it using PySpark on Databricks, and creates analytics-ready datasets. It's honestly been a great learning experience for implementing some advanced DE concepts like SCD Type 2, incremental processing, and proper data modeling.

## Tech Stack

- **Data Platform**: Azure Databricks (because that's what I use at work and wanted more hands-on practice)
- **Processing**: PySpark for transformations
- **Storage**: Delta Lake (Bronze → Silver → Gold medallion architecture)
- **Modeling**: SQL + dbt (planned for future iterations)
- **Orchestration**: Databricks Workflows
- **Target Warehouse**: Currently using Databricks SQL, but designed to work with Snowflake/Synapse/Redshift

## Project Status

🟢 **Active Development** - Started December 2025

**What's Working:**
- ✅ Chess.com API data ingestion (player profiles, game history)
- ✅ Lichess API integration  
- ✅ Bronze layer (raw data landing)
- ✅ Silver layer transformations with data quality checks
- ✅ Player rating history with SCD Type 2 implementation
- ✅ Basic analytics queries

**In Progress:**
- 🔄 dbt models for dimensional modeling
- 🔄 Opening repertoire analysis
- 🔄 Performance dashboards

**Future Plans:**
- ⏳ CI/CD with GitHub Actions
- ⏳ Databricks Asset Bundles for deployment
- ⏳ Terraform for infrastructure management
- ⏳ Real-time game stream processing

## Project Structure

```
.
├── README.md
├── databricks/
│   ├── notebooks/
│   │   ├── 01_bronze_ingestion/
│   │   │   ├── ingest_chesscom_games.py
│   │   │   └── ingest_lichess_games.py
│   │   ├── 02_silver_transforms/
│   │   │   ├── cleanse_game_data.py
│   │   │   ├── player_rating_scd2.py       # SCD Type 2 implementation
│   │   │   └── game_analysis_enrichment.py
│   │   └── 03_gold_models/
│   │       ├── fact_games.py
│   │       ├── dim_players.py
│   │       └── dim_openings.py
│   ├── sql/
│   │   ├── analytics/
│   │   │   ├── player_performance_trends.sql
│   │   │   ├── opening_success_rates.sql
│   │   │   └── rating_progression.sql
│   │   └── data_quality/
│   │       └── dq_checks.sql
│   └── utils/
│       ├── chess_api_client.py
│       └── data_quality_framework.py
├── dbt/                          # dbt project (future)
│   ├── models/
│   ├── macros/
│   └── dbt_project.yml
├── config/
│   └── pipeline_config.yaml
└── docs/
    ├── architecture.md
    └── data_model.md
```

## Data Sources

### Chess.com Published Data API
- **Endpoint**: `https://api.chess.com/pub/`
- **Rate Limits**: Reasonable for personal use, no auth required for public data
- **Data**: Player profiles, game archives (PGN format), stats, clubs
- **Docs**: https://www.chess.com/news/view/published-data-api

### Lichess API  
- **Endpoint**: `https://lichess.org/api/`
- **Rate Limits**: More generous than Chess.com
- **Data**: Games (PGN/JSON), player stats, puzzles, tournaments
- **Format**: Supports both PGN and JSON, which is nice for processing
- **Docs**: https://lichess.org/api

## Architecture Overview

**Bronze Layer (Raw)**
- Landing zone for API responses
- Stored as-is in JSON format with metadata
- Partitioned by date and source

**Silver Layer (Cleansed)**
- Parsed PGN games into structured format
- Data quality validation (null checks, valid moves, etc)
- Deduplicated based on game ID
- Player rating history with SCD Type 2 (tracks rating changes over time)

**Gold Layer (Analytics-Ready)**
- Dimensional model (star schema)
- Pre-aggregated metrics
- Business logic applied (ELO rating categories, opening classifications)

## Key Features

### 1. SCD Type 2 for Player Ratings

Track how player ratings change over time:
```sql
SELECT
    player_key,
    username,
    rating_blitz,
    rating_rapid,
    rating_bullet,
    valid_from,
    valid_to,
    is_current
FROM silver.player_ratings_scd2
WHERE username = 'your_username'
ORDER BY valid_from DESC;
```

### 2. Game Analysis Enrichment

Each game gets analyzed for:
- Opening name and ECO code
- Game phase durations (opening/middlegame/endgame)
- Accuracy score (if available from platform)
- Critical moments/blunders

### 3. Cross-Platform Analytics

Compare performance across Chess.com and Lichess:
- Rating correlation
- Playing style differences  
- Time control preferences

## Sample Insights

Some interesting things I've learned about my own games:

- My win rate with the Sicilian Defense is surprisingly low (need to work on that)
- I play significantly better in the morning (8-11 AM) vs late night
- My average game length in rapid is 38 moves
- I have a 62% win rate as White vs 48% as Black

## Setup Instructions

### Prerequisites
- Azure subscription with Databricks workspace
- Chess.com and/or Lichess account
- Python 3.10+
- Databricks CLI (optional, for deployment)

### Local Development

1. Clone the repo:
```bash
git clone https://github.com/ramaksha89/chess-game-analytics-platform.git
cd chess-game-analytics-platform
```

2. Install dependencies:
```bash
pip install -r requirements.txt  # TODO: add this file
```

3. Update config with your details:
```yaml
# config/pipeline_config.yaml
chess_com:
  username: "your_username"
lichess:
  username: "your_username"
  # token: optional for private data
```

4. Run ingestion notebook in Databricks or locally

### Databricks Deployment

```bash
# Using Databricks CLI
databricks workspace import_dir \
  databricks/notebooks \
  /Workspace/Users/your@email.com/chess-analytics

# Create job for scheduled runs
# (UI-based for now, Asset Bundles coming later)
```

## SQL Examples

### Opening Performance
```sql
-- Which openings work best for me?
SELECT
    opening_name,
    COUNT(*) as games_played,
    SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) as wins,
    ROUND(AVG(CASE WHEN result = 'win' THEN 1.0 
                   WHEN result = 'draw' THEN 0.5 
                   ELSE 0 END) * 100, 1) as win_rate_pct,
    ROUND(AVG(accuracy), 1) as avg_accuracy
FROM gold.fact_games
WHERE color = 'white'
    AND time_class = 'rapid'
GROUP BY opening_name
HAVING games_played >= 10  -- only openings I've played enough
ORDER BY win_rate_pct DESC
LIMIT 20;
```

### Rating Progression
```sql
-- My rating journey over time
SELECT
    DATE(game_end_time) as date,
    AVG(my_rating) as avg_rating,
    COUNT(*) as games_played
FROM gold.fact_games
WHERE time_class = 'rapid'
    AND game_end_time >= '2024-01-01'
GROUP BY DATE(game_end_time)
ORDER BY date;
```

## Challenges & Learnings

**API Rate Limiting**  
Chess.com can be finicky with rate limits. Implemented exponential backoff and caching to handle this gracefully.

**PGN Parsing**  
PGN (Portable Game Notation) parsing was trickier than expected. Used `python-chess` library which handles most edge cases, but still had to add custom logic for non-standard variations.

**SCD Type 2 Implementation**  
Getting SCD Type 2 right in PySpark took a few iterations. The merge logic for detecting rating changes needs careful handling of the valid_from/valid_to dates.

**Data Volume**  
Even for a casual player, you accumulate a lot of games. Bronze layer quickly grows if you're pulling full game archives. Had to implement incremental loading after the initial full load.

## Future Enhancements

1. **Real-time Stream Processing**  
   - Use Lichess streaming API for live game analysis
   - Databricks Structured Streaming for real-time metrics

2. **ML/AI Integration**  
   - Predict game outcomes based on opening moves
   - Identify playing patterns and suggest improvements
   - Stockfish integration for position evaluation

3. **Multi-Sport Template**  
   - Abstract the pattern to work for other games/sports
   - Similar APIs exist for poker, League of Legends, etc.

4. **Dashboard**  
   - Power BI or Tableau dashboard
   - Or maybe a simple Streamlit app for quick visualizations

## Contributing

This is a personal learning project, but if you're interested in similar analysis for your games, feel free to fork it! The code should be pretty straightforward to adapt for your own use.

If you spot any bugs or have suggestions, open an issue. I'm always learning.

## License

MIT - do whatever you want with this

## Connect

If you're working on similar projects or just want to chat about data engineering / chess, feel free to reach out!

- GitHub: [@ramaksha89](https://github.com/ramaksha89)
- LinkedIn: [Connect with me] https://www.linkedin.com/in/ram-marreddy

---

**Note**: This project is for educational/personal use. All game data comes from public APIs with proper attribution to Chess.com and Lichess.
