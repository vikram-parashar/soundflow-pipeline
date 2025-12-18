CREATE TABLE bronze.auth_events (
    event_ts timestamptz NOT NULL,
    user_id integer,
    session_id integer,
    success boolean,
    level text,
    city text,
    state text,
    payload jsonb NOT NULL,
    ingestion_ts timestamptz DEFAULT now()
);

CREATE TABLE bronze.listen_events (
    event_ts timestamptz NOT NULL,
    user_id integer,
    session_id integer,
    artist text,
    song text,
    level text,
    auth text,
    city text,
    state text,
    payload jsonb NOT NULL,
    ingestion_ts timestamptz DEFAULT now()
);

CREATE TABLE bronze.page_view_events (
    event_ts timestamptz NOT NULL,
    user_id integer,
    session_id integer,
    page text,
    method text,
    status integer,
    auth text,
    level text,
    artist text,
    song text,
    city text,
    state text,
    payload jsonb NOT NULL,
    ingestion_ts timestamptz DEFAULT now()
);

CREATE TABLE bronze.status_change_events (
    event_ts timestamptz NOT NULL,
    user_id integer,
    session_id integer,
    auth text,
    level text,
    city text,
    state text,
    payload jsonb NOT NULL,
    ingestion_ts timestamptz DEFAULT now()
);

