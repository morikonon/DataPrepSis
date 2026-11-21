-- Normalised schema: one row per race, driver and constructor; results reference them by key.

CREATE TABLE IF NOT EXISTS races (
    race_id           TEXT    PRIMARY KEY,                  -- '<season>_<round>', e.g. '2024_05'
    season            INTEGER NOT NULL CHECK (season >= 1950),
    round             INTEGER NOT NULL CHECK (round BETWEEN 1 AND 30),
    race_name         TEXT    NOT NULL,
    race_date         TEXT    NOT NULL,                     -- ISO 8601 date
    race_datetime_utc TEXT    NOT NULL,                     -- ISO 8601 timestamp, UTC
    circuit_id        TEXT    NOT NULL,
    circuit_name      TEXT    NOT NULL,
    locality          TEXT    NOT NULL,
    country           TEXT    NOT NULL,
    UNIQUE (season, round)
);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id     TEXT    PRIMARY KEY,
    driver_code   TEXT    NOT NULL CHECK (length(driver_code) = 3),
    driver_number INTEGER,
    driver_name   TEXT    NOT NULL,
    nationality   TEXT,
    date_of_birth TEXT
);

CREATE TABLE IF NOT EXISTS constructors (
    constructor_id   TEXT PRIMARY KEY,
    constructor_name TEXT NOT NULL,
    nationality      TEXT,
    team_id          TEXT NOT NULL,                         -- lineage key: alfa -> sauber, alphatauri -> rb
    team_name        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS results (
    result_id        TEXT    PRIMARY KEY,                   -- record_id: '<season>_<round>_<driver_id>'
    race_id          TEXT    NOT NULL REFERENCES races (race_id),
    driver_id        TEXT    NOT NULL REFERENCES drivers (driver_id),
    constructor_id   TEXT    NOT NULL REFERENCES constructors (constructor_id),
    grid             INTEGER NOT NULL CHECK (grid >= 0),
    grid_effective   INTEGER NOT NULL CHECK (grid_effective >= 1),
    position         INTEGER NOT NULL CHECK (position >= 1),
    position_text    TEXT    NOT NULL,
    is_classified    INTEGER NOT NULL CHECK (is_classified IN (0, 1)),
    points           REAL    NOT NULL CHECK (points >= 0),
    laps             INTEGER NOT NULL CHECK (laps >= 0),
    status           TEXT    NOT NULL,
    status_group     TEXT    NOT NULL CHECK (status_group IN ('Finished', 'Lapped', 'DNF', 'DNS', 'DSQ')),
    result_category  TEXT    NOT NULL,
    positions_gained INTEGER,                               -- NULL when not classified
    race_time_ms     INTEGER,                               -- NULL when no time was recorded
    gap_to_winner_s  REAL,
    fastest_lap_rank INTEGER,
    fastest_lap_time TEXT,
    fastest_lap_s    REAL,
    lap_pace_gap_pct REAL,
    driver_age       INTEGER NOT NULL,
    UNIQUE (race_id, driver_id)
);

CREATE INDEX IF NOT EXISTS idx_results_driver ON results (driver_id);
CREATE INDEX IF NOT EXISTS idx_results_constructor ON results (constructor_id);

CREATE VIEW IF NOT EXISTS v_results AS
SELECT r.result_id, ra.season, ra.round, ra.race_name, ra.race_date, ra.country,
       d.driver_code, d.driver_name, c.constructor_name, c.team_name,
       r.grid, r.grid_effective, r.position, r.is_classified, r.points, r.laps, r.status, r.status_group,
       r.result_category, r.positions_gained, r.race_time_ms, r.gap_to_winner_s,
       r.fastest_lap_s, r.lap_pace_gap_pct, r.driver_age
FROM results r
JOIN races ra       ON ra.race_id = r.race_id
JOIN drivers d      ON d.driver_id = r.driver_id
JOIN constructors c ON c.constructor_id = r.constructor_id;
