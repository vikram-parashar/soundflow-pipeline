import json
import psycopg2
import psycopg2.extras
import os
import time

BATCH_LIMIT = 1000


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


def flush_batch(conn, sql, batch):
    if len(batch) == 0:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            sql,
            batch,
            page_size=len(batch),
        )

    conn.commit()


def extract_auth_events(conn):
    insert_sql = """
    INSERT INTO bronze.auth_events 
    ( event_ts, user_id, session_id, success, level, city, state, payload)
    VALUES
    ( to_timestamp(%s / 1000.0), %s, %s, %s, %s, %s, %s, %s::jsonb)
    """
    batch = []
    insert_cnt = 0

    with open("data/auth_events", "r") as f:
        for ln, line in enumerate(f, start=1):
            if not line.strip():
                continue

            try:
                event = json.loads(line)

                batch.append(
                    (
                        event.get("ts"),
                        event.get("userId"),
                        event.get("sessionId"),
                        event.get("success"),
                        event.get("level"),
                        event.get("city"),
                        event.get("state"),
                        json.dumps(event),
                    )
                )

                if len(batch) >= BATCH_LIMIT:
                    flush_batch(conn, insert_sql, batch)
                    insert_cnt += BATCH_LIMIT
                    print(f"inserted {insert_cnt} in bronze.auth_events")
                    batch.clear()

            except json.JSONDecodeError as e:
                print(f"Invalid JSON at line {ln}: {e}")

            except Exception as e:
                conn.rollback()
                raise RuntimeError(
                    f"Failed processing file data/auth_events at line {ln}"
                ) from e

    # Flush remaining records
    flush_batch(conn, insert_sql, batch)
    insert_cnt += len(batch)
    print(f"inserted {insert_cnt} in bronze.auth_events")


def extract_page_view_events(conn):
    insert_sql = """
    INSERT INTO bronze.page_view_events
    (event_ts, user_id, session_id,page,method,status, auth,level,artist,song, city, state, payload)
    VALUES
    ( to_timestamp(%s / 1000.0), %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s, %s::jsonb)
    """
    batch = []
    insert_cnt = 0

    with open("data/page_view_events", "r") as f:
        for ln, line in enumerate(f, start=1):
            if not line.strip():
                continue

            try:
                event = json.loads(line)

                batch.append(
                    (
                        event.get("ts"),
                        event.get("userId"),
                        event.get("sessionId"),
                        event.get("page"),
                        event.get("method"),
                        event.get("status"),
                        event.get("auth"),
                        event.get("level"),
                        event.get("artist"),
                        event.get("song"),
                        event.get("city"),
                        event.get("state"),
                        json.dumps(event),
                    )
                )

                if len(batch) >= BATCH_LIMIT:
                    flush_batch(conn, insert_sql, batch)
                    insert_cnt += BATCH_LIMIT
                    print(f"inserted {insert_cnt} in bronze.page_view_events")
                    batch.clear()

            except json.JSONDecodeError as e:
                print(f"Invalid JSON at line {ln}: {e}")

            except Exception as e:
                conn.rollback()
                raise RuntimeError(
                    f"Failed processing file data/page_view_events at line {ln}"
                ) from e

    # Flush remaining records
    flush_batch(conn, insert_sql, batch)
    insert_cnt += len(batch)
    print(f"inserted {insert_cnt} in bronze.page_view_events")


def extract_status_change_events(conn):
    insert_sql = """
    INSERT INTO bronze.status_change_events
    (event_ts, user_id, session_id, auth,level, city, state, payload)
    VALUES
    ( to_timestamp(%s / 1000.0), %s, %s, %s, %s, %s, %s, %s::jsonb)
    """
    batch = []
    insert_cnt = 0

    with open("data/status_change_events", "r") as f:
        for ln, line in enumerate(f, start=1):
            if not line.strip():
                continue

            try:
                event = json.loads(line)

                batch.append(
                    (
                        event.get("ts"),
                        event.get("userId"),
                        event.get("sessionId"),
                        event.get("auth"),
                        event.get("level"),
                        event.get("city"),
                        event.get("state"),
                        json.dumps(event),
                    )
                )

                if len(batch) >= BATCH_LIMIT:
                    flush_batch(conn, insert_sql, batch)
                    insert_cnt += BATCH_LIMIT
                    print(f"inserted {insert_cnt} in bronze.status_change_events")
                    batch.clear()

            except json.JSONDecodeError as e:
                print(f"Invalid JSON at line {ln}: {e}")

            except Exception as e:
                conn.rollback()
                raise RuntimeError(
                    f"Failed processing file data/status_change_events at line {ln}"
                ) from e
    # Flush remaining records
    flush_batch(conn, insert_sql, batch)
    insert_cnt += len(batch)
    print(f"inserted {insert_cnt} in bronze.status_change_events")


def extract_listen_events(conn):
    insert_sql = """
    INSERT INTO bronze.listen_events 
    ( event_ts, user_id, session_id,artist,song, level,auth, city, state, payload)
    VALUES
    ( to_timestamp(%s / 1000.0), %s, %s, %s, %s, %s, %s,%s,%s, %s::jsonb)
    """
    batch = []
    insert_cnt = 0

    with open("data/listen_events", "r") as f:
        for ln, line in enumerate(f, start=1):
            if not line.strip():
                continue

            try:
                event = json.loads(line)

                batch.append(
                    (
                        event.get("ts"),
                        event.get("userId"),
                        event.get("sessionId"),
                        event.get("artist"),
                        event.get("song"),
                        event.get("level"),
                        event.get("auth"),
                        event.get("city"),
                        event.get("state"),
                        json.dumps(event),
                    )
                )

                if len(batch) >= BATCH_LIMIT:
                    flush_batch(conn, insert_sql, batch)
                    insert_cnt += BATCH_LIMIT
                    print(f"inserted {insert_cnt} in bronze.listen_events")
                    batch.clear()

            except json.JSONDecodeError as e:
                print(f"Invalid JSON at line {ln}: {e}")

            except Exception as e:
                conn.rollback()
                raise RuntimeError(
                    f"Failed processing file data/listen_events at line {ln}"
                ) from e

    # Flush remaining records
    flush_batch(conn, insert_sql, batch)
    insert_cnt += len(batch)
    print(f"inserted {insert_cnt} in bronze.listen_events")


def truncate_bronze(conn):
    sql = """
    TRUNCATE TABLE
        bronze.auth_events,
        bronze.listen_events,
        bronze.status_change_events,
        bronze.page_view_events
    RESTART IDENTITY;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def main():
    conn = get_pg_conn()
    try:
        truncate_bronze(conn)
        extract_auth_events(conn)
        extract_listen_events(conn)
        extract_status_change_events(conn)
        extract_page_view_events(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
