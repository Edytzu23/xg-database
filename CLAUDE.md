# CLAUDE.md — xPts Engine

## Project Overview
Expected Fantasy Points (xPts) calculation engine for **UCL Fantasy** (UEFA) and **FIFA World Cup Fantasy**.
Uses domestic league statistics to predict fantasy points per player per match, with and without opponent adjustment.

## Golden Rule
Backend accuracy first. The model must be statistically sound before any UI work begins.

## Tech Stack
- **Language**: Python 3.11+
- **Database**: SQLite (local MVP), migrate to PostgreSQL for Vercel deploy later
- **Data sources**: FotMob (primary, reuse FF Dashboard pipeline), FBref (secondary, for stats FotMob lacks)
- **Stats libraries**: pandas, numpy, scipy (Poisson model)
- **API framework**: FastAPI (same as FF Dashboard, for future UI integration)
- **Frontend** (later): HTML/JS with Tailwind, embeddable in existing Vercel site

## Scoring Systems

### UCL Fantasy
| Action | GK | DEF | MID | FWD |
|---|---|---|---|---|
| Appearance | 1 | 1 | 1 | 1 |
| 60+ minutes | +1 | +1 | +1 | +1 |
| Goal scored | 6 | 6 | 5 | 4 |
| Assist | 3 | 3 | 3 | 3 |
| Clean sheet (60+ min) | 4 | 4 | 1 | 0 |
| Every 2 goals conceded | -1 | -1 | - | - |
| Every 3 saves | 1 | - | - | - |
| Penalty save | 5 | 5 | 5 | 5 |
| Penalty won | 2 | 2 | 2 | 2 |
| Penalty missed | -2 | -2 | -2 | -2 |
| Penalty conceded | -1 | -1 | -1 | -1 |
| Goal from outside box | +1 | +1 | +1 | +1 |
| Every 3 ball recoveries | 1 | 1 | 1 | 1 |
| Player of the Match | 3 | 3 | 3 | 3 |
| Yellow card | -1 | -1 | -1 | -1 |
| Red card | -3 | -3 | -3 | -3 |
| Own goal | -2 | -2 | -2 | -2 |
| Captain | 2x | 2x | 2x | 2x |

### FIFA World Cup Fantasy (based on 2022, 2026 TBC)
Same as UCL except:
- 60+ minutes = **+2** (total 3 not 2)
- **No** goal from outside box bonus
- **No** ball recoveries bonus
- **No** Player of the Match bonus
- Has Maximum Captain chip and Bench Boost chip

## xPts Model — Component Breakdown

```
xPts = xMinPts + xGoalPts + xAssistPts + xCSPts + xGCPts + xSavePts
     + xRecoveryPts + xBonusPts + xDiscPts + xPenPts + xOutsideBoxPts
```

### Components

| Component | Formula | Notes |
|---|---|---|
| **xMinPts** | `1 × P(play≥1min) + 1 × P(play≥60min)` | UCL: max 2pts. WC: `1 × P(≥1) + 2 × P(≥60)` max 3pts. Bayesian-smoothed from last 6-15 matches |
| **xGoalPts** | `npxG/90 × goal_pts[pos] × min_proportion` | Weighted: 30% last season + 30% current season + 40% last 6 matches. Add penalty boost if penalty taker |
| **xAssistPts** | `xA/90 × 3 × min_proportion` | Same weighting as goals |
| **xCSPts** | `P(CS) × P(≥60min) × cs_pts[pos]` | Poisson model: P(0 goals conceded). Blend: 70% fixture-bucket + 30% historical rate |
| **xGCPts** | `-(expected_goals_conceded / 2) × P(≥60min)` | GK/DEF only. From team xGA and opponent xG |
| **xSavePts** | `expected_saves/90 / 3 × min_proportion` | GK only. Historical saves rate |
| **xRecoveryPts** | `recoveries/90 / 3 × min_proportion` | UCL only (not in WC Fantasy) |
| **xBonusPts** | `P(POTM) × 3` | UCL only. Proxy: top xG+xA contributors in winning teams |
| **xDiscPts** | `-(yellow_rate × 1 + red_rate × 3) × P(appearance)` | No fixture adjustment |
| **xPenPts** | `is_taker × P(team_wins_pen) × 0.76 × goal_pts[pos]` | 0.76 = avg penalty xG. Plus penalty_won bonus (2pts) |
| **xOutsideBoxPts** | `outside_box_goal_rate × npxG/90 × 1` | UCL only. Historical ratio of goals from outside box |

### Two Output Modes
- **xPts_raw**: Sum of all components, no opponent adjustment (pure player value)
- **xPts_adjusted**: xPts_raw × competition_factor × opponent_factor

### Opponent Adjustment (3-Layer System)
1. **Baseline**: Domestic league per90 stats (weighted rolling average)
2. **Competition scaling**: `competition_factor = elo_avg_player_league / elo_avg_competition`
3. **Opponent specific**: `opponent_factor = elo_avg_league / elo_opponent`

## Data Sources & Pipeline

### FotMob (primary) — reuse from FF Dashboard
- xG, xA, goals, assists, minutes, position per player
- Team xG, xGA, clean sheets, form splits (season/home/away/last5)
- Already has: caching (6h TTL), parallel fetching, buildId management
- Reference implementation: `f:/Claude Folder/FF Dashboard/scouting.py`

### FBref (secondary) — new scraping needed
- Ball recoveries per 90
- Saves per 90 (GK)
- Yellow/red card rates
- Tackles, interceptions (defensive contribution)
- Shots from outside the box rate
- Penalty taker identification
- Use `soccerdata` or custom scraper with respectful rate limits

### ClubElo.com — Elo ratings
- CSV download, free, no API key needed
- Used for competition_factor and opponent_factor calculations

### football-data.org — fixtures & schedule
- Free API key (10 req/min)
- UCL and World Cup fixtures, match schedule, results

### The Odds API (optional)
- 500 free requests/month
- Pre-match odds for win/draw/loss probabilities
- Only if free tier is sufficient

## Database Schema (SQLite)

Core tables:
- `players` — id, name, team, position, fotmob_id, fbref_id
- `teams` — id, name, code, league, fotmob_id, elo_rating
- `leagues` — id, name, country, elo_avg
- `competitions` — id, name, type (ucl/worldcup), scoring_system
- `player_stats` — player_id, season, competition, per90 stats (xG, xA, recoveries, saves, cards, etc.)
- `fixtures` — id, competition_id, home_team_id, away_team_id, date, status
- `xpts_predictions` — player_id, fixture_id, each component value, xpts_raw, xpts_adjusted, computed_at
- `penalty_takers` — team_id, player_id, priority (1=first choice)

## Project Structure
```
XG DATABASE/
  CLAUDE.md
  requirements.txt
  db/
    schema.sql          # SQLite schema
    xpts.db             # SQLite database file
  src/
    __init__.py
    config.py           # Scoring systems, league configs, constants
    models/
      __init__.py
      poisson.py        # Poisson distribution for CS probability, match outcome
      xpts.py           # Main xPts calculator — all components
      adjustments.py    # Elo-based opponent/competition adjustments
    data/
      __init__.py
      fotmob.py         # FotMob scraper (adapted from FF Dashboard scouting.py)
      fbref.py          # FBref scraper for supplementary stats
      elo.py            # ClubElo data fetcher
      fixtures.py       # football-data.org fixture fetcher
    db/
      __init__.py
      connection.py     # SQLite connection manager
      queries.py        # Insert/update/select helpers
    pipeline/
      __init__.py
      update.py         # Main update pipeline: fetch → compute → store
      scheduler.py      # Scheduled auto-updates (cron-style)
  api/
    __init__.py
    main.py             # FastAPI app
    routes.py           # API endpoints
  tests/
    test_poisson.py
    test_xpts.py
    test_adjustments.py
  scripts/
    seed_teams.py       # Initial team/league/player seeding
    backfill.py         # Historical data backfill
```

## Coverage Requirements
- All UCL 2025-26 teams (36 teams in league phase, knockout participants)
- All FIFA World Cup 2026 qualified nations (48 teams)
- Players: all squad members where stats are available
- Domestic leagues for stats: Premier League, La Liga, Bundesliga, Serie A, Ligue 1, Liga Portugal, Eredivisie, and others as needed

## Update Frequency
- **Player stats**: Daily during active competition, weekly otherwise
- **Elo ratings**: Weekly
- **Fixtures**: On competition schedule release + daily during matchdays
- **xPts recalculation**: After every stats update

## Hard Rules
- Never hardcode team/player lists — always fetch dynamically from data sources
- Always store raw per90 stats separately from computed xPts (reproducibility)
- Every xPts prediction must be traceable: which stats, which model version, which timestamp
- Respect rate limits on all scrapers — add delays, use caching
- FotMob buildId changes on deploy — always fetch dynamically, never hardcode
- Test the Poisson model against known outcomes before trusting predictions
- Both scoring systems (UCL + WC) must be configurable, not duplicated code
- Component weights and formulas must be in config, not buried in code

## Development Phases

### Phase 1: Data Foundation
- SQLite schema + connection layer
- FotMob adapter (port from FF Dashboard)
- FBref scraper for supplementary stats
- Elo data fetcher
- Seed script for all UCL teams

### Phase 2: xPts Model
- Poisson model for clean sheet / goals conceded probabilities
- All 11 xPts components implemented
- Opponent adjustment system (3-layer Elo)
- Both scoring systems (UCL + WC)
- Validation against historical matchday results

### Phase 3: Pipeline & Automation
- Auto-update scheduler
- Fixture-aware predictions (pre-matchday batch)
- Data freshness monitoring

### Phase 4: API & UI
- FastAPI endpoints for xPts queries
- Frontend dashboard (sortable table, filters by position/team)
- Integration path to existing Vercel FF Dashboard site
