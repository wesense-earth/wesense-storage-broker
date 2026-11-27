
# Background archive work must not slow interactive readers (the Respiro map and
# dashboard). ClickHouse pauses lower-priority queries (higher number) while
# higher-priority ones run, and one thread leaves the other cores free.
BACKGROUND_QUERY_SETTINGS = {"priority": 10, "max_threads": 1}
