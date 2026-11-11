WITH RECURSIVE calendar AS (SELECT  DATE('2024-01-01') AS date_calendar, 0  AS n

UNION ALL

SELECT DATE(date_calendar, '+1 day'),
n+1
FROM calendar
WHERE  n<365),



table_dau AS (
SELECT DATE(dt) AS date_dau,
    strftime('%Y-%m',dt) AS month,
    COUNT(distinct user) AS dau
FROM data_store
WHERE 1=1
AND dt >='2024-01-01' AND dt <'2025-01-01'
GROUP BY 1,2),


table_mau AS ( SELECT  strftime('%Y-%m',dt) AS month,
    COUNT(distinct user)  AS mau
FROM data_store
WHERE 1=1
AND dt >='2024-01-01' AND dt <'2025-01-01'
GROUP BY 1)


SELECT c.date_calendar,
d.month,
    COALESCE(d.dau,0) AS dau,
     COALESCE(m.mau,0) AS mau,
     round(d.dau*100.0/m.mau,2) AS sticky_factor_percent

FROM calendar c
LEFT JOIN table_dau d ON c.date_calendar=d.date_dau
LEFT JOIN table_mau m ON d.month=m.month
