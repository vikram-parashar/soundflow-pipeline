import psycopg2
import os
import time


def transform_auth_events(conn):
    sql = """
    INSERT INTO silver.auth_events (
        event_ts,
        user_id,
        session_id,
        success,
        level,
        city,
        state,
        zip,
        user_agent,
        lat,
        lon,
        item_in_session,
        ingestion_ts
    )
    SELECT
        event_ts,
        user_id,
        session_id,
        success,
        level,
        city,
        state,
        payload->>'zip',
        payload->>'userAgent',
        (payload->>'lat')::double precision,
        (payload->>'lon')::double precision,
        (payload->>'itemInSession')::integer,
        ingestion_ts
    FROM bronze.auth_events;
    """

    with conn.cursor() as cur:
        cur.execute(sql)

    conn.commit()


def transform_listen_events(conn):
    sql = """
    INSERT INTO silver.listen_events (
        event_ts,
        user_id,
        session_id,
        artist,
        song,
        duration,
        level,
        auth,
        city,
        state,
        zip,
        user_agent,
        lat,
        lon,
        item_in_session,
        ingestion_ts
    )
    SELECT
        event_ts,
        user_id,
        session_id,
        artist,
        song,
        (payload->>'duration')::double precision,
        level,
        auth,
        city,
        state,
        payload->>'zip',
        payload->>'userAgent',
        (payload->>'lat')::double precision,
        (payload->>'lon')::double precision,
        (payload->>'itemInSession')::integer,
        ingestion_ts
    FROM bronze.listen_events;
    """

    with conn.cursor() as cur:
        cur.execute(sql)

    conn.commit()


def transform_page_view_events(conn):
    sql = """
    INSERT INTO silver.page_view_events (
        event_ts,
        user_id,
        session_id,
        page,
        method,
        status,
        auth,
        level,
        artist,
        song,
        duration,
        city,
        state,
        zip,
        user_agent,
        lat,
        lon,
        item_in_session,
        ingestion_ts
    )
    SELECT
        event_ts,
        user_id,
        session_id,
        page,
        method,
        status,
        auth,
        level,
        artist,
        song,
        (payload->>'duration')::double precision,
        city,
        state,
        payload->>'zip',
        payload->>'userAgent',
        (payload->>'lat')::double precision,
        (payload->>'lon')::double precision,
        (payload->>'itemInSession')::integer,
        ingestion_ts
    FROM bronze.page_view_events;
    """

    with conn.cursor() as cur:
        cur.execute(sql)

    conn.commit()


def transform_status_change_events(conn):
    sql = """
    INSERT INTO silver.status_change_events (
        event_ts,
        user_id,
        session_id,
        auth,
        level,
        city,
        state,
        zip,
        user_agent,
        lat,
        lon,
        item_in_session,
        ingestion_ts
    )
    SELECT
        event_ts,
        user_id,
        session_id,
        auth,
        level,
        city,
        state,
        payload->>'zip',
        payload->>'userAgent',
        (payload->>'lat')::double precision,
        (payload->>'lon')::double precision,
        (payload->>'itemInSession')::integer,
        ingestion_ts
    FROM bronze.status_change_events;
    """

    with conn.cursor() as cur:
        cur.execute(sql)

    conn.commit()


def run_transforms(conn):
    transform_auth_events(conn)
    transform_listen_events(conn)
    transform_page_view_events(conn)
    transform_status_change_events(conn)


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
        run_transforms(conn)
    finally:
        conn.close()
