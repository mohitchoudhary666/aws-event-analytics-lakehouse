-- The `event_date` string partition predicate is intentionally included in every
-- time-bounded query so Athena can skip unrelated S3 partitions.

-- Daily revenue and order volume.
SELECT event_date AS day,
       ROUND(SUM(quantity * unit_price_usd), 2) AS revenue_usd,
       COUNT_IF(event_type = 'purchase') AS orders
FROM event_analytics.events_curated
WHERE event_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
GROUP BY event_date
ORDER BY event_date;

-- Session-level conversion funnel: count a session at each step at most once.
WITH session_signals AS (
    SELECT session_id,
           MAX(IF(event_type = 'product_view', 1, 0)) AS viewed,
           MAX(IF(event_type = 'add_to_cart', 1, 0)) AS carted,
           MAX(IF(event_type = 'checkout', 1, 0)) AS checked_out,
           MAX(IF(event_type = 'purchase', 1, 0)) AS purchased
    FROM event_analytics.events_curated
    WHERE event_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
    GROUP BY session_id
)
SELECT 1 AS stage_order, 'product_view' AS stage, COUNT_IF(viewed = 1) AS sessions FROM session_signals
UNION ALL
SELECT 2, 'add_to_cart', COUNT_IF(carted = 1) FROM session_signals
UNION ALL
SELECT 3, 'checkout', COUNT_IF(checked_out = 1) FROM session_signals
UNION ALL
SELECT 4, 'purchase', COUNT_IF(purchased = 1) FROM session_signals
ORDER BY stage_order;

-- Revenue and average order value by region.
SELECT region,
       COUNT_IF(event_type = 'purchase') AS orders,
       ROUND(SUM(quantity * unit_price_usd), 2) AS revenue_usd,
       ROUND(SUM(quantity * unit_price_usd) / NULLIF(COUNT_IF(event_type = 'purchase'), 0), 2) AS average_order_value_usd
FROM event_analytics.events_curated
WHERE event_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
  AND event_type = 'purchase'
GROUP BY region
ORDER BY revenue_usd DESC;

-- Top product categories by sales.
SELECT category,
       COUNT(*) AS purchase_events,
       SUM(quantity) AS units_sold,
       ROUND(SUM(quantity * unit_price_usd), 2) AS revenue_usd
FROM event_analytics.events_curated
WHERE event_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
  AND event_type = 'purchase'
GROUP BY category
ORDER BY revenue_usd DESC
LIMIT 10;
