with t_cohort as (
	select
		dt,
		user,
		sale,
		-- Формируем когорту, находя самую первую покупку для каждого клиента
		-- и извлекая из нее месяц и год
		strftime('%Y-%m',
            first_value(dt) over(partition by user order by dt)
        ) as cohort
	from data_store
	where dt between '2024-01-01' and '2025-01-01' )

	select cohort,
	count(distinct user) as cnt,
	-- Суммируем всю выручку по данной когорте за все предыдущие периоды, включая текущий месяц
	sum(case when dt < '2024-02-01' then sale end) as "2024-01",
	sum(case when dt < '2024-03-01' then sale end) as "2024-02",
	sum(case when dt < '2024-04-01' then sale end) as "2024-03",
	sum(case when dt < '2024-05-01' then sale end) as "2024-04",
	sum(case when dt < '2024-06-01' then sale end) as "2024-05",
	sum(case when dt < '2024-07-01' then sale end) as "2024-06",
  sum(case when dt < '2024-08-01' then sale end) as "2024-07",
  sum(case when dt < '2024-09-01' then sale end) as "2024-08",
  sum(case when dt < '2024-10-01' then sale end) as "2024-09",
  sum(case when dt < '2024-11-01' then sale end) as "2024-10",
  sum(case when dt < '2024-12-01' then sale end) as "2024-11",
  sum(case when dt < '2025-01-01' then sale end) as "2024-12"
from t_cohort
group by cohort
order by cohort
