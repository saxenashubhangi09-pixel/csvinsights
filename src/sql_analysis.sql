
SELECT
    COUNT(*) AS total_events,
    SUM(cost_eur) AS total_cost_eur,
    SUM(downtime_min) AS total_downtime_minutes,
    AVG(downtime_min) AS avg_downtime_minutes,
    SUM(CASE WHEN reason = 'Unplanned Breakdown' THEN 1 ELSE 0 END) AS unplanned_breakdowns
FROM maintenance_events;
