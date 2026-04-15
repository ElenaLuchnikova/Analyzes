with t_cohort as (
	select
		dt,
		user,
		sale,
		-- Формируем когорту, находя самую первую покупку для каждого клиента
		-- и извлекая из нее месяц и год
		strftime('%Y-%m',
            first_value(dt) over(partition by user order by dt)
        ) as cohort,
        julianday(dt) - julianday(first_value(dt) over(partition by user order by dt)) as diff

	from data_store
	where dt between '2024-01-01' and '2025-01-01' )

  select cohort,
  count(distinct user) as cnt,
  max(diff) as max_diff,
  round(sum(case when diff=0 then sale end)/count(distinct user),2) as "0",
  case when max(diff)> 0 then round(sum(case when diff<=7 then sale end)/count(distinct user),2) end as "7",
  case when max(diff)> 7 then round(sum(case when diff<=14 then sale end)/count(distinct user),2) end as "14",
  case when max(diff)> 14 then round(sum(case when diff<=30 then sale end)/count(distinct user),2) end as "30",
  case when max(diff)>30 then round(sum(case when diff<=60 then sale end)/count(distinct user),2) end as "60",
  case when max(diff)>60 then round(sum(case when diff<=90 then sale end)/count(distinct user),2) end as "90",
  case when max(diff)>90 then round(sum(case when diff<=180 then sale end)/count(distinct user),2) end as "180",
  case when max(diff)>180 then round(sum(case when diff<=270 then sale end)/count(distinct user),2) end as "270",
  case when max(diff)>270 then round(sum(case when diff<=365 then sale end)/count(distinct user),2) end as "360"
  from t_cohort
  group by cohort
  order by cohort
