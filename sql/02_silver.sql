CREATE TABLE silver.auth_events (
    event_ts timestamptz NOT NULL,
    user_id integer,
    session_id integer,
    success boolean,
    level text,
    city text,
    state text,
    zip text,
    user_agent text,
    lat double precision,
    lon double precision,
    item_in_session integer,
    ingestion_ts timestamptz NOT NULL
);

CREATE TABLE silver.listen_events (
    event_ts timestamptz NOT NULL,
    user_id integer,
    session_id integer,
    artist text,
    song text,
    duration double precision,
    level text,
    auth text,
    city text,
    state text,
    zip text,
    user_agent text,
    lat double precision,
    lon double precision,
    item_in_session integer,
    ingestion_ts timestamptz NOT NULL
);

CREATE TABLE silver.page_view_events (
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
    duration double precision,
    city text,
    state text,
    zip text,
    user_agent text,
    lat double precision,
    lon double precision,
    item_in_session integer,
    ingestion_ts timestamptz NOT NULL
);

CREATE TABLE silver.status_change_events (
    event_ts timestamptz NOT NULL,
    user_id integer,
    session_id integer,
    auth text,
    level text,
    city text,
    state text,
    zip text,
    user_agent text,
    lat double precision,
    lon double precision,
    item_in_session integer,
    ingestion_ts timestamptz NOT NULL
);

