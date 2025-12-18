CREATE TABLE bronze.auth_events (
    event_ts           TIMESTAMPTZ    NOT NULL,
    user_id            INTEGER,
    session_id         INTEGER,

    success            BOOLEAN,
    level              TEXT,

    city               TEXT,
    state              TEXT,

    payload            JSONB          NOT NULL,

    ingestion_ts       TIMESTAMPTZ    DEFAULT now()
);

CREATE TABLE bronze.listen_events (
    event_ts        TIMESTAMPTZ    NOT NULL,

    user_id         INTEGER,
    session_id      INTEGER,

    artist          TEXT,
    song            TEXT,
    level           TEXT,
    auth            TEXT,

    city            TEXT,
    state           TEXT,

    payload         JSONB          NOT NULL,

    ingestion_ts    TIMESTAMPTZ    DEFAULT now()
);

CREATE TABLE bronze.page_view_events (
    event_ts        TIMESTAMPTZ    NOT NULL,

    user_id         INTEGER,
    session_id      INTEGER,

    page            TEXT,
    method          TEXT,
    status          INTEGER,
    auth            TEXT,
    level           TEXT,

    artist          TEXT,
    song            TEXT,

    city            TEXT,
    state           TEXT,

    payload         JSONB          NOT NULL,

    ingestion_ts    TIMESTAMPTZ    DEFAULT now()
);

CREATE TABLE bronze.status_change_events (
    event_ts        TIMESTAMPTZ    NOT NULL,

    user_id         INTEGER,
    session_id      INTEGER,

    auth            TEXT,
    level           TEXT,

    city            TEXT,
    state           TEXT,

    payload         JSONB          NOT NULL,

    ingestion_ts    TIMESTAMPTZ    DEFAULT now()
);
