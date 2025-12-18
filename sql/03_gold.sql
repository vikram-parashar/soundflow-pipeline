CREATE TABLE gold.daily_user_activity (
    activity_date date NOT NULL,
    user_id integer NOT NULL,
    sessions_count integer NOT NULL,
    listens_count integer NOT NULL,
    page_views_count integer NOT NULL,
    PRIMARY KEY (activity_date, user_id)
);

CREATE TABLE gold.daily_song_plays (
    play_date date NOT NULL,
    artist text NOT NULL,
    song text NOT NULL,
    plays_count integer NOT NULL,
    unique_users integer NOT NULL,
    PRIMARY KEY (play_date, artist, song)
);

CREATE TABLE gold.user_sessions (
    session_id integer NOT NULL,
    user_id integer NOT NULL,
    session_start_ts timestamptz NOT NULL,
    session_end_ts timestamptz NOT NULL,
    session_duration_s integer NOT NULL,
    events_count integer NOT NULL,
    listens_count integer NOT NULL,
    city text,
    state text,
    PRIMARY KEY (user_id, session_id)
);

CREATE TABLE gold.subscription_funnel_daily (
    event_date date NOT NULL,
    user_id integer NOT NULL,
    first_level text NOT NULL,
    last_level text NOT NULL,
    had_auth_event boolean NOT NULL,
    PRIMARY KEY (event_date, user_id)
);

CREATE TABLE gold.daily_geo_activity (
    activity_date date NOT NULL,
    state text NOT NULL,
    city text NOT NULL,
    active_users integer NOT NULL,
    total_events integer NOT NULL,
    total_listens integer NOT NULL,
    PRIMARY KEY (activity_date, state, city)
);

CREATE TABLE gold.user_lifetime_metrics (
    user_id integer NOT NULL,
    first_seen_ts timestamptz NOT NULL,
    last_seen_ts timestamptz NOT NULL,
    total_sessions integer NOT NULL,
    total_listens integer NOT NULL,
    total_page_views integer NOT NULL,
    days_active integer NOT NULL,
    PRIMARY KEY (user_id)
);

