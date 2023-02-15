create schema foodie_fi;
use foodie_fi;
create table plans
 ( plan_id   integer, plan_name varchar(13), price decimal(5,2));
 insert into plans 
 (plan_id,plan_name,price)
 value 
 ('0','trial','0'),
 ('1', 'basic monthly','9.90'),
 ('2','pro monthly','19.90'),
 ('3','pro annual','199'),
 ('4', 'churn' , null);
 create table subscriptions
 (customer_id integer   , plan_id  integer , start_date  date);
 insert into   subscriptions ( customer_id,plan_id,start_date)
 value 
 ('1',	'0'	,'2020-08-01'),
('1','1','2020-08-08'),
('2','0','2020-09-20'),
('2','3','2020-09-27'),
('11','0','2020-11-19'),
('11','4','2020-11-26'),
('13','0','2020-12-15'),
('13',	'1','2020-12-22'),
('13','2','2021-03-29'),
('15','0','2020-03-17'),
('15','2','2020-03-24'),
('15','4','2020-04-29'),
('16','0','2020-05-31'),
('16','1','2020-06-07'),
('16','3','2020-10-21'),
('18','0','2020-07-06'),
('18','2','2020-07-13'),
('19','0','2020-06-22'),
('19','2','2020-06-29'),
('19','3','2020-08-29');
--  A. Customer Journey
-- 
select
s.customer_id,f.plan_id,
f.plan_name,s.start_date from plans
 as f inner join subscriptions as s 
 on f.plan_id =s.plan_id where s.customer_id in
 (1,2,11,13,15,16,18,19);
  -- try three more sample
  --  customer # 1
 select
s.customer_id,f.plan_id,
f.plan_name,s.start_date from plans
 as f inner join subscriptions as s 
 on f.plan_id =s.plan_id where s.customer_id in
 (1);
 --  customer #13
  select
s.customer_id,f.plan_id,
f.plan_name,s.start_date from plans
 as f inner join subscriptions as s 
 on f.plan_id =s.plan_id where s.customer_id in
 (13);
 -- customer #19 
  select
s.customer_id,f.plan_id,
f.plan_name,s.start_date from plans
 as f inner join subscriptions as s 
 on f.plan_id =s.plan_id where s.customer_id in
 (19);
-- B. Data Analysis Questions
select count(distinct customer_id) from subscriptions;
-- Q 2 
SELECT
 month (start_date )AS months,
 count(customer_id) as num_customers
FROM subscriptions 
group by months;

-- Q . 3
SELECT 
  p.plan_id,
  p.plan_name,
  COUNT(*) AS events
FROM subscriptions s
JOIN plans p
  ON s.plan_id = p.plan_id
WHERE s.start_date >= '2020-08-01'
GROUP BY p.plan_id, p.plan_name
ORDER BY p.plan_id;
--  q 4 
SELECT 
  COUNT(*) AS churn_count,
  ROUND(count(*)*100  / (
    SELECT COUNT(DISTINCT customer_id) 
    FROM subscriptions),1) AS churn_percentage
FROM subscriptions s
JOIN plans p
  ON s.plan_id = p.plan_id
WHERE s.plan_id = 4;
--  Q 5
 WITH ranking AS (
SELECT 
  s.customer_id, 
  s.plan_id, 
  p.plan_name,
  ROW_NUMBER() OVER (
    PARTITION BY s.customer_id 
    ORDER BY s.plan_id) AS plan_rank 
FROM subscriptions s
JOIN plans p
  ON s.plan_id = p.plan_id)
SELECT 
  COUNT(*) AS churn_count,
  ROUND(100 * COUNT(*) / (
    SELECT COUNT(DISTINCT customer_id) 
    FROM subscriptions),0) AS churn_percentage
FROM ranking
WHERE plan_id = 4 
  AND plan_rank = 2 
--  Q 6 

WITH  ext_plan_cte AS (
SELECT 
      *, 
       LEAD(plan_id, 1) OVER( -- Offset by 1 to retrieve the immediate row's value below 
       PARTITION BY customer_id 
    ORDER BY plan_id) as next_plan
FROM subscriptions)
SELECT 
  next_plan, 
  COUNT(*) AS conversions,
  ROUND( COUNT(*) *100/(
    SELECT COUNT(DISTINCT customer_id) 
    FROM subscriptions),1) AS conversion_percentage
FROM next_plan_cte
WHERE next_plan IS NOT NULL 
  AND plan_id = 0
GROUP BY next_plan
ORDER BY next_plan;

-- ques 7

WITH next_plan AS(
SELECT 
  customer_id, 
  plan_id, 
  start_date,
  LEAD(start_date, 1) OVER(PARTITION BY customer_id ORDER BY start_date) as next_date
FROM subscriptions
WHERE start_date <= '2020-12-31'
),
-- Find customer breakdown with existing plans on or after 31 Dec 2020
customer_breakdown AS (
  SELECT 
    plan_id, 
    COUNT(DISTINCT customer_id) AS customers
  FROM next_plan
  WHERE 
    (next_date IS NOT NULL AND (start_date < '2020-12-31' 
      AND next_date > '2020-12-31'))
    OR (next_date IS NULL AND start_date < '2020-12-31')
  GROUP BY plan_id)

SELECT plan_id, customers, 
  ROUND(100 * customers / (
    SELECT COUNT(DISTINCT customer_id) 
    FROM subscriptions),1) AS percentage
FROM customer_breakdown
GROUP BY plan_id, customers
ORDER BY plan_id;





 -- Question no 8
SELECT 
  COUNT(DISTINCT customer_id) AS unique_customer
FROM subscriptions WHERE plan_id = 3
  AND start_date <= '2020-12-31';
  --  Question no 9
  WITH trial_plan AS 
  (SELECT 
    customer_id, 
    start_date AS trial_date
  FROM subscriptions
  WHERE plan_id = 0
),
-- Filter results to customers at pro annual plan = 3
annual_plan AS
  (SELECT 
    customer_id, 
    start_date AS annual_date
  FROM subscriptions
  WHERE plan_id = 3
)
SELECT 
  ROUND(AVG(annual_date - trial_date),0) AS avg_days_to_upgrade
FROM trial_plan tp
JOIN annual_plan ap
  ON tp.customer_id = ap.customer_id;
  -- Question no  10
  WITH trial_plan AS 
  (SELECT 
    customer_id, 
    start_date AS trial_date
  FROM subscriptions
  WHERE plan_id = 0
),
-- Filter results to customers at pro annual plan = 3
annual_plan AS
  (SELECT customer_id, start_date AS annual_date
  FROM subscriptions WHERE plan_id = 3
),
-- Sort values above in buckets of 12 with range of 30 days each
day_period AS 
  (SELECT 
    datediff(annual_date , trial_date ) AS avg_days_to_upgrad
    FROM trial_plan tp
 left JOIN annual_plan ap
    ON tp.customer_id = ap.customer_id 
    where annual_date is not null
    ),
bins AS 
  (SELECT 
    *,floor( avg_days_to_upgrad/30) as bins from day_period)
    select
    concat((bins*30)+1,'-',(bins+1) *30,'days') As days,count( avg_days_to_upgrad) as total from bins
    group by bins;
   
--  Question no 11
WITH next_plan_cte AS (
  SELECT 
    customer_id, 
    plan_id, 
    start_date,
    LEAD(plan_id, 1) OVER(
      PARTITION BY customer_id 
      ORDER BY plan_id) as next_plan
  FROM subscriptions)

SELECT 
  COUNT(*) AS downgraded
FROM next_plan_cte
WHERE start_date <= '2020-12-31'
  AND plan_id = 2 
  AND next_plan = 1;
  
  
-- C. Challenge Payment Question
with 
join_table as    --  create base table
 (
  select
        s.customer_id,
		s.plan_id,
		p.plan_name,
        s.start_date  payment_date ,
        s.start_date,
		lead(s.start_date,1) over(partition by s.customer_id order by s.start_date,s.plan_id) next_date,
        p.price amount 
        from subscriptions s
       left join plans p on p.plan_id=s.plan_id
       ),
  new_join as -- fileter dateset
  (    
  select 
       customer_id,
       plan_id,
       plan_name,
       payment_date,
	   start_date ,
       case when next_date is null or next_date > '2020-12-31' then '2020-12-31' else next_date end next_date ,
 amount
 from join_table
 where plan_name  not in ('traial','churn')
 ),
 new_join1 as  -- add new column 1 month before  next_date
 (
 select
      customer_id,
       plan_id,
       plan_name,
       payment_date,
       start_date ,
       next_date,
		dateadd(month, -1,next_date) next_date1, amount
       from new_join
),
date_CTE   --  RECURSIVE FUNCTION (FOR PYMENT_DATE)
 as 
 (
select
      customer_id,
      plan_id,
      plan_name,
      start_date ,
      payment_date= (select top  start_date from new_join1 where customer_id =a.customer_id and plan_id =a.plan_id),
      next_date,
      next_date1,
      amount
from new_join1 a
		union all
select
	customer_id,
    plan_id,
    plan_name,
    start_date ,
    dateadd  = (M,1,payment_date)payment_date,
    next_date,
    next_date1,
    amount
from  date_CTE b 
where payment_date < next_date1 and plan_id !=3
)
select
    customer_id,
    plan_name,
    payment_date ,
    amount,
  rank () over (partition by  customer_id  order by customer_id ,plan_id,payment_date) payment_order
from  date_CTE
WHERE YEAR(payment_date)=2020
order by customer_id, plan_id,payment_date;

  



