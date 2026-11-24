-- One row per question; its tags live in question_tags (many-to-many), tracked tools are tags with a tool name.

CREATE TABLE IF NOT EXISTS questions (
    question_id              INTEGER PRIMARY KEY,
    title                    TEXT    NOT NULL CHECK (length(title) >= 15),
    link                     TEXT    NOT NULL UNIQUE,
    score                    INTEGER NOT NULL,
    view_count               INTEGER NOT NULL CHECK (view_count >= 0),
    answer_count             INTEGER NOT NULL CHECK (answer_count >= 0),
    is_answered              INTEGER NOT NULL CHECK (is_answered IN (0, 1)),
    has_accepted_answer      INTEGER NOT NULL CHECK (has_accepted_answer IN (0, 1)),
    is_edited                INTEGER NOT NULL CHECK (is_edited IN (0, 1)),
    is_closed                INTEGER NOT NULL CHECK (is_closed IN (0, 1)),
    status                   TEXT    NOT NULL CHECK (status IN ('Accepted', 'Answered, not accepted', 'Unanswered',
                                                                'Closed')),
    closed_reason            TEXT,                              -- NULL while the question is open
    accepted_answer_id       INTEGER,                           -- NULL without an accepted answer
    creation_at              TEXT    NOT NULL,                  -- ISO 8601, UTC
    last_activity_at         TEXT    NOT NULL CHECK (last_activity_at >= creation_at),
    last_edit_at             TEXT,
    closed_at                TEXT,
    first_answer_at          TEXT    CHECK (first_answer_at >= creation_at),
    accepted_answer_at       TEXT,
    hours_to_first_answer    REAL    CHECK (hours_to_first_answer >= 0),
    hours_to_accepted_answer REAL    CHECK (hours_to_accepted_answer >= 0),
    answered_within_24h      INTEGER NOT NULL CHECK (answered_within_24h IN (0, 1)),
    views_per_day            REAL    NOT NULL,
    log_views                REAL    NOT NULL,
    owner_user_id            INTEGER,                           -- NULL for deleted accounts
    owner_user_type          TEXT    NOT NULL CHECK (owner_user_type IN ('registered', 'unregistered',
                                                                         'does_not_exist')),
    owner_reputation         INTEGER CHECK (owner_reputation >= 1),
    reputation_band          TEXT    NOT NULL,
    creation_month           TEXT    NOT NULL,                  -- 'YYYY-MM'
    creation_weekday         TEXT    NOT NULL,
    creation_hour_utc        INTEGER NOT NULL CHECK (creation_hour_utc BETWEEN 0 AND 23),
    title_length             INTEGER NOT NULL,
    tag_count                INTEGER NOT NULL CHECK (tag_count BETWEEN 1 AND 5),
    tool_count               INTEGER NOT NULL CHECK (tool_count >= 1),
    collected_at             TEXT    NOT NULL,
    CHECK (has_accepted_answer = (accepted_answer_id IS NOT NULL)),
    CHECK (is_closed = (closed_at IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS question_tags (
    question_id  INTEGER NOT NULL REFERENCES questions (question_id) ON DELETE CASCADE,
    tag          TEXT    NOT NULL,
    tag_position INTEGER NOT NULL CHECK (tag_position BETWEEN 1 AND 5),
    tool         TEXT,                                          -- display name if the tag is a tracked tool
    PRIMARY KEY (question_id, tag)
);

CREATE INDEX IF NOT EXISTS idx_question_tags_tool ON question_tags (tool);
CREATE INDEX IF NOT EXISTS idx_questions_month ON questions (creation_month);

CREATE VIEW IF NOT EXISTS v_question_tools AS
SELECT t.tool, q.*
FROM questions q
JOIN question_tags t ON t.question_id = q.question_id
WHERE t.tool IS NOT NULL;
