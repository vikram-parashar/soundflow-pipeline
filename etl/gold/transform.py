import psycopg2
import os
import time


def truncate_gold(conn):
    sql = """
    TRUNCATE TABLE
        gold.daily_user_activity,
        gold.daily_song_plays,
        gold.user_sessions,
        gold.subscription_funnel_daily,
        gold.daily_geo_activity,
        gold.user_lifetime_metrics
    RESTART IDENTITY;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def build_daily_user_activity(conn):
    sql = """
    INSERT INTO gold.daily_user_activity (
        activity_date,
        user_id,
        sessions_count,
        listens_count,
        page_views_count
    )
    SELECT
        activity_date,
        user_id,
        COUNT(DISTINCT session_id) AS sessions_count,
        SUM(listens_count)         AS listens_count,
        SUM(page_views_count)      AS page_views_count
    FROM (
        SELECT
            date(event_ts) AS activity_date,
            user_id,
            session_id,
            COUNT(*) AS listens_count,
            0        AS page_views_count
        FROM silver.listen_events
        WHERE user_id IS NOT NULL
        GROUP BY 1, 2, 3

        UNION ALL

        SELECT
            date(event_ts) AS activity_date,
            user_id,
            session_id,
            0,
            COUNT(*)
        FROM silver.page_view_events
        WHERE user_id IS NOT NULL
        GROUP BY 1, 2, 3
    ) t
    GROUP BY activity_date, user_id;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def build_daily_song_plays(conn):
    sql = """
    INSERT INTO gold.daily_song_plays (
        play_date,
        artist,
        song,
        plays_count,
        unique_users
    )
    SELECT
        date(event_ts) AS play_date,
        artist,
        song,
        COUNT(*)                  AS plays_count,
        COUNT(DISTINCT user_id)   AS unique_users
    FROM silver.listen_events
    WHERE artist IS NOT NULL
      AND song IS NOT NULL
    GROUP BY 1, 2, 3;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def build_user_sessions(conn):
    sql = """
    INSERT INTO gold.user_sessions (
        session_id,
        user_id,
        session_start_ts,
        session_end_ts,
        session_duration_s,
        events_count,
        listens_count,
        city,
        state
    )
    SELECT
        session_id,
        user_id,
        MIN(event_ts) AS session_start_ts,
        MAX(event_ts) AS session_end_ts,
        EXTRACT(EPOCH FROM MAX(event_ts) - MIN(event_ts))::INTEGER
            AS session_duration_s,
        COUNT(*)      AS events_count,
        SUM(listens)  AS listens_count,
        MAX(city)     AS city,
        MAX(state)    AS state
    FROM (
        SELECT session_id, user_id, event_ts, city, state, 0 AS listens
        FROM silver.page_view_events
        WHERE user_id IS NOT NULL

        UNION ALL

        SELECT session_id, user_id, event_ts, city, state, 1
        FROM silver.listen_events
        WHERE user_id IS NOT NULL

        UNION ALL

        SELECT session_id, user_id, event_ts, city, state, 0
        FROM silver.auth_events
        WHERE user_id IS NOT NULL

        UNION ALL

        SELECT session_id, user_id, event_ts, city, state, 0
        FROM silver.status_change_events
        WHERE user_id IS NOT NULL
    ) t
    GROUP BY session_id, user_id;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def build_subscription_funnel_daily(conn):
    sql = """
    INSERT INTO gold.subscription_funnel_daily (
        event_date,
        user_id,
        first_level,
        last_level,
        had_auth_event
    )
    WITH events AS (
        SELECT
            date(event_ts) AS event_date,
            event_ts,
            user_id,
            level,
            TRUE AS had_auth_event
        FROM silver.auth_events
        WHERE user_id IS NOT NULL

        UNION ALL

        SELECT
            date(event_ts),
            event_ts,
            user_id,
            level,
            FALSE
        FROM silver.status_change_events
        WHERE user_id IS NOT NULL
    ),
    windowed AS (
        SELECT
            event_date,
            user_id,
            FIRST_VALUE(level) OVER w AS first_level,
            LAST_VALUE(level)  OVER w AS last_level,
            had_auth_event
        FROM events
        WINDOW w AS (
            PARTITION BY event_date, user_id
            ORDER BY event_ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
    )
    SELECT
        event_date,
        user_id,
        first_level,
        last_level,
        BOOL_OR(had_auth_event) AS had_auth_event
    FROM windowed
    GROUP BY
        event_date,
        user_id,
        first_level,
        last_level;
    """

    with conn.cursor() as cur:
        cur.execute(sql)

    conn.commit()


def build_daily_geo_activity(conn):
    sql = """
    INSERT INTO gold.daily_geo_activity (
        activity_date,
        state,
        city,
        active_users,
        total_events,
        total_listens
    )
    SELECT
        date(event_ts) AS activity_date,
        state,
        city,
        COUNT(DISTINCT user_id) AS active_users,
        COUNT(*)                AS total_events,
        SUM(listens)            AS total_listens
    FROM (
        SELECT event_ts, user_id, city, state, 0 AS listens
        FROM silver.page_view_events

        UNION ALL

        SELECT event_ts, user_id, city, state, 1
        FROM silver.listen_events

        UNION ALL

        SELECT event_ts, user_id, city, state, 0
        FROM silver.auth_events

        UNION ALL

        SELECT event_ts, user_id, city, state, 0
        FROM silver.status_change_events
    ) t
    WHERE city IS NOT NULL
      AND state IS NOT NULL
    GROUP BY 1, 2, 3;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def build_user_lifetime_metrics(conn):
    sql = """
    INSERT INTO gold.user_lifetime_metrics (
        user_id,
        first_seen_ts,
        last_seen_ts,
        total_sessions,
        total_listens,
        total_page_views,
        days_active
    )
    SELECT
        user_id,
        MIN(event_ts)                        AS first_seen_ts,
        MAX(event_ts)                        AS last_seen_ts,
        COUNT(DISTINCT session_id)           AS total_sessions,
        SUM(listens)                         AS total_listens,
        SUM(page_views)                      AS total_page_views,
        COUNT(DISTINCT date(event_ts))       AS days_active
    FROM (
        SELECT user_id, session_id, event_ts, 0 AS listens, 1 AS page_views
        FROM silver.page_view_events

        UNION ALL

        SELECT user_id, session_id, event_ts, 1, 0
        FROM silver.listen_events

        UNION ALL

        SELECT user_id, session_id, event_ts, 0, 0
        FROM silver.auth_events

        UNION ALL

        SELECT user_id, session_id, event_ts, 0, 0
        FROM silver.status_change_events
    ) t
    WHERE user_id IS NOT NULL
    GROUP BY user_id;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def run_gold_transforms(conn):
    truncate_gold(conn)
    build_daily_user_activity(conn)
    build_daily_song_plays(conn)
    build_user_sessions(conn)
    build_subscription_funnel_daily(conn)
    build_daily_geo_activity(conn)
    build_user_lifetime_metrics(conn)


def get_pg_conn():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB", "soundflow")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")

    attempt = 0
    while True:
        try:
            return psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password,
                connect_timeout=5,
            )

        except psycopg2.OperationalError as exc:
            attempt += 1
            if attempt >= 5:
                raise RuntimeError(
                    f"Failed to connect to PostgreSQL after {attempt} attempts"
                ) from exc

            sleep_time = 2 * (2 ** (attempt - 1))
            time.sleep(sleep_time)


if __name__ == "__main__":
    conn = get_pg_conn()
    try:
        run_gold_transforms(conn)
    finally:
        conn.close()
