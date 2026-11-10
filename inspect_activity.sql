SELECT
    pid,
    application_name,
    state,
    wait_event_type,
    wait_event,
    backend_start,
    LEFT(query, 150) AS query
FROM pg_stat_activity
WHERE datname = current_database()
ORDER BY backend_start;
