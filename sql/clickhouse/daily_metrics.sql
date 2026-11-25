SELECT
    count() AS processed_events,
    uniqExact(user_id) AS dau,
    countIf(event_type = 'VIEW_STARTED') AS views_started,
    countIf(event_type = 'VIEW_FINISHED') AS views_finished,
    if(countIf(event_type = 'VIEW_FINISHED') = 0, NULL,
       avgIf(progress_seconds, event_type = 'VIEW_FINISHED')) AS avg_watch_seconds,
    countIf(event_type = 'VIEW_FINISHED') / nullIf(countIf(event_type = 'VIEW_STARTED'), 0) AS conversion
FROM cinema.events FINAL
WHERE toDate(timestamp) = {day:Date}
