create table user_events (
	user_id UInt32,
	event_type String,
	points_spent UInt32,
	event_time Datetime
) Engine = MergeTree()
order by (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;

create table agg_user_events (
	event_type String,
	event_date Date,
	unique_users AggregateFunction(uniq, UInt32),
    total_spent AggregateFunction(sum, UInt32),
    total_actions AggregateFunction(count, UInt8)
) ENGINE = AggregatingMergeTree() ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW user_events_mv TO agg_user_events
AS SELECT 
event_type,
toDate(event_time) AS event_date,
uniqState(user_id) AS unique_users,
sumState (points_spent) AS total_spent,
countState (event_type) AS total_actions
FROM user_events
GROUP BY event_type, toDate(event_time)
ORDER BY (toDate(event_time), event_type);

INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),

(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),

(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),

(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),

(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),

(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());


SELECT 
    toDate(event_time) AS event_date,
    event_type,
    countDistinct(user_id) AS unique_users,
    sum(points_spent) AS total_spent,
    count() AS total_actions
FROM user_events
GROUP BY event_date, event_type
ORDER BY event_date ASC;

SELECT
    day_0,
    countDistinct(user_id) AS total_users_day_0,
    countDistinct(returned_user_id) AS returned_in_7_days,
    round(100.0 * countDistinct(returned_user_id) / countDistinct(user_id), 2) AS retention_7d_percent
FROM
(
  
    SELECT
        toDate(event_time) AS day_0,
        user_id
    FROM user_events
    WHERE event_type = 'login'
    GROUP BY day_0, user_id
) AS first_day_users
LEFT JOIN
(
    SELECT
        toDate(ue.event_time) AS return_date,
        ue.user_id AS returned_user_id,
        fd.day_0
    FROM
    (
        SELECT
            toDate(event_time) AS day_0,
            user_id
        FROM user_events
        WHERE event_type = 'login'
        GROUP BY day_0, user_id
    ) AS fd
    JOIN user_events AS ue
        ON ue.user_id = fd.user_id
        AND toDate(ue.event_time) > fd.day_0
        AND toDate(ue.event_time) <= fd.day_0 + INTERVAL 7 DAY
) AS returned_users
ON first_day_users.user_id = returned_users.returned_user_id
AND first_day_users.day_0 = returned_users.day_0
GROUP BY day_0
ORDER BY day_0;

SELECT
    toDate(event_time) AS event_date,
    event_type,
    countDistinct(user_id) AS unique_users,
    sum(points_spent) AS total_spent,
    count() AS total_actions
FROM user_events
GROUP BY event_date, event_type
ORDER BY event_date, event_type;




