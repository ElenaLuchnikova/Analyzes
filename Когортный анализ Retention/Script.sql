with t1 as (
	select
		dt,
		user,
		sale,
		-- Формируем когорту, находя самую первую покупку для каждого клиента
		-- и извлекая из нее месяц и год
		strftime('%Y-%m',
            first_value(dt) over(partition by user order by dt)
        ) as cohort,
        first_value(dt) over(partition by user order by dt) as first_day,
        julianday(dt) - julianday(first_value(dt) over(partition by user order by dt)) as diff

	from data_store
	where dt between '2024-01-01' and '2025-01-01'),

  t2 as (select cohort,
  diff,
  count(distinct user) as cnt
  from t1
  where diff in (0,1,3,7, 14,30,60,90,180,270,365)
  group by cohort, diff
  order by cohort, diff)



  select cohort,
  max(case when diff=0 then cnt end) as "0",
  max(case when diff=1 then cnt end) as "1",
  max(case when diff=3 then cnt end) as "3",
  max(case when diff=7 then cnt end) as "7",
  max(case when diff=14 then cnt end) as "14",
  max(case when diff=30 then cnt end) as "30",
  max(case when diff=60 then cnt end) as "60",
  max(case when diff=90 then cnt end) as "90",
  max(case when diff=180 then cnt end) as "180",
  max(case when diff=270 then cnt end) as "270",
  max(case when diff=360 then cnt end) as "360"
  from t2
  group by cohort
