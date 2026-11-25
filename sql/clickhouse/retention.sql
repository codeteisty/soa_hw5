WITH first_views AS
(
    SELECT user_id, min(toDate(timestamp)) AS cohort_date
    FROM cinema.events FINAL
    WHERE event_type = 'VIEW_STARTED'
    GROUP BY user_id
), activity AS
(
    SELECT e.user_id, groupUniqArray(toDate(e.timestamp)) AS active_days
    FROM cinema.events AS e FINAL
    INNER JOIN first_views AS c ON e.user_id = c.user_id
    WHERE c.cohort_date = {day:Date}
      AND toDate(e.timestamp) BETWEEN {day:Date} AND addDays({day:Date}, 7)
    GROUP BY e.user_id
)
SELECT
    day_number,
    count() AS cohort_size,
    countIf(has(active_days, addDays({day:Date}, day_number))) AS returned_users,
    if(day_number > 0 AND addDays({day:Date}, day_number) > {as_of:Date}, NULL,
       countIf(has(active_days, addDays({day:Date}, day_number))) / count()) AS retention
FROM activity
ARRAY JOIN range(8) AS day_number
GROUP BY day_number
ORDER BY day_number
