create database retail_sales;
use retail_sales;


select * from customers
select * from orders 
select * from payments
select * from products
select * from ratings
select * from stores



-- Data Cleaning


-- deleting duplicate rows from payments (615 records deleted)
delete tbl from(
select *, row_number() over(partition by order_id, payment_type, payment_value order by (select null)) rnk from payments) tbl
where rnk > 1


-- deleting duplicate rows from ratings (350 records deleted)
delete tbl from(
select *, row_number() over(partition by order_id, customer_satisfaction_score  order by (select null)) rnk from ratings) tbl
where rnk > 1 


-- deleting duplicate rows from products (1 records deleted)
delete tbl from(
select *, row_number() over(partition by storeid, seller_city, seller_state, region  order by (select null)) rnk from stores) tbl
where rnk > 1 


-- converting date of order table in date time format

alter table orders
alter column bill_date_timestamp datetime


-- converting the category of #na to unknown (623 rows converted to unknown)
update products
set Category = 'Unknown'
where category = '#N/A'


-- orderid of payment which are not present in orders (811 rows deleted)
delete tbl from
(select * from payments
where order_id not in (select order_id from orders)) tbl


-- orderid of ratings which are not present in orders (776 rows deleted)
delete tbl from
(select * from ratings
where order_id not in (select order_id from orders)) tbl


-- updating quantity to 1 for orderid having total_amount and payment not equal
update orders
set Quantity = 1
where order_id in 
(select o.order_id from orders o join payments p on o.order_id = p.order_id
group by o.order_id
having sum(total_amount) != sum(payment_value))


-- updating total_amount for the new quantity updated
update orders
set total_amount = (Quantity * mrp) - Discount




-- Question 1

-- High Level Metrics

-- Number of orders
select count(distinct(order_id)) total_no_of_orders 
from orders

-- Total discount
select sum(discount) total_discount from orders

-- Average discount per customer
select sum(discount)/count(distinct Customer_id) avg_discount_per_customer from orders 


-- Average discount per order
select sum(discount)/count(distinct order_id) avg_discount_per_order from orders


-- Average bill value
select sum(total_amount)/count(distinct Customer_id) avg_bill_value from orders

-- Average transactions per customer
SELECT (CAST(COUNT(order_id) AS FLOAT) / COUNT(DISTINCT customer_id)) AS average_transactions_per_customer
FROM orders;

-- Average sales per customer
select sum(total_amount)/count(distinct order_id) avg_sales_per_customers from orders 


-- Average profit per customer
select sum(total_amount - (cost_per_unit * Quantity))/ count(distinct Customer_id) profit from orders


-- Average number of categories per order
select cast(sum(cnt)/count(order_id) as float) from
(select order_id, count(distinct(Category)) cnt from orders o join products pd
on o.product_id = pd.product_id
group by order_id) tbl1


-- Average number of items per order
select sum(Quantity)/count(distinct order_id) from orders


-- Number of customers
select count(distinct(customer_id)) from orders


-- Total Revenue, Total Profit,  Total Cost, Total quantity, Total products, Total categories, 
-- Total stores, Total locations, Total Regions, Total channels, Total payment methods

select sum(total_amount) Total_Revenue,  sum(total_amount - (cost_per_unit * Quantity)) Total_Profit,
sum(Cost_Per_Unit * Quantity) Total_Cost, sum(Quantity) Total_Quantity, count(distinct(Channel)) Total_Channels from orders

select count(distinct(o.product_id)) Total_Products, count(distinct(category)) Total_Categories from orders o left join 
products pd on o.product_id = pd.product_id

select count(distinct(storeid)) Total_Stores, count(distinct(seller_city)) Total_seller_Location, 
count(distinct(region)) Total_Region from orders o left join stores s on o.Delivered_StoreID = s.StoreID

select count(distinct(payment_type)) Total_Paymnet_Method from payments 

-- Average number of days between two transactions

with cte1 as (
select customer_id,  bill_date_timestamp, ROW_NUMBER() over(partition by customer_id order by bill_date_timestamp) rw_num
 from orders),
 cte2 as(
 select t1.customer_id, t1.bill_date_timestamp, datediff(day, t1.bill_date_timestamp, t2.bill_date_timestamp) days_diff from cte1 t1
join cte1 t2 on t1.customer_id = t2.customer_id and t1.rw_num + 1 = t2.rw_num
where t1.rw_num > 1)

select customer_id, avg(days_diff) avg_diff_ from cte2
group by customer_id


-- Percentage of profit per order

select order_id, round(sum(total_amount - (Cost_Per_Unit * Quantity)),2) * 100.00/
(select round(sum(total_amount - (Cost_Per_Unit * Quantity)),2) from orders) profit_percent from orders
group by order_id


-- Percentage of discount per order

select order_id, (sum(Discount) * 100.00 /(select sum(Discount) from orders)) discount_percent from orders
group by order_id
order by discount_percent desc

-- Repeat purchase rate

with cte1 as (
select customer_id, (count(distinct order_id) - 1) cnt_ord from orders
group by Customer_id
having count(distinct order_id) > 1)

select sum(cte1.cnt_ord) * 100.00 /(select count(distinct(order_id)) from orders) from cte1


-- Repeat customer percentage rate

with cte1 as (
select customer_id, count(distinct order_id) cnt_ord from orders
group by Customer_id
having count(distinct order_id) > 1)

select count(cte1.customer_id) * 100.00 /(select count(distinct(customer_id)) from orders) from cte1



-- One time buyers percentage 

with cte1 as (
select customer_id, count(distinct order_id) cnt_ord from orders
group by Customer_id
having count(distinct order_id) = 1)

select count(cte1.customer_id) * 100.00 /(select count(distinct(customer_id)) from orders) from cte1


-- BEHAVIOUR
select 'One time buyer' Type_buyer,round(sum(total_amount),2) total_sales, count(distinct order_id) no_of_orders  , sum(quantity) total_quantity from orders
where Customer_id in (select customer_id from orders
group by Customer_id
having count(distinct order_id) = 1)
union
select 'Repeated buyer' Type_buyer,round(sum(total_amount),2) total_sales, count(distinct order_id) no_of_orders  , sum(quantity) total_quantity from orders
where Customer_id in (select customer_id from orders
group by Customer_id
having count(distinct order_id) > 1)


-- Understanding how many new customers acquired every month (who made transaction first time in the data

select year(Bill_date_timestamp) year_, MONTH(Bill_date_timestamp) month_, count(Customer_id) new_cust 
FROM (
select customer_id, bill_date_timestamp, 
rank() over(partition by customer_id  order by bill_date_timestamp) rnk from orders) tbl1 
where rnk = 1
group by year(Bill_date_timestamp), MONTH(Bill_date_timestamp)
order by year(Bill_date_timestamp), MONTH(Bill_date_timestamp)


-- Understand the retention of customers on month on month basis 

select year(tm.bill_date_timestamp) as year_, month(tm.bill_date_timestamp) as month_, 
count(lm.customer_id) from orders tm
left join orders lm
on tm.customer_id = lm.customer_id and 
datediff(month, lm.bill_date_timestamp, tm.bill_date_timestamp) = 1
group by year(tm.bill_date_timestamp), month(tm.bill_date_timestamp)


-- Understand the trends/seasonality of sales, quantity by category, region, store, 
-- channel, payment method etc…

select year(bill_date_timestamp) yr, month(bill_date_timestamp) mth ,round(sum(total_amount),2) total_sales from orders
group by year(bill_date_timestamp), month(bill_date_timestamp)
order by year(bill_date_timestamp), month(bill_date_timestamp)

select year(bill_date_timestamp) yr, month(bill_date_timestamp) mth, channel , sum(Quantity) Total_quantity ,round(sum(total_amount),2) total_sales from orders
group by year(bill_date_timestamp), month(bill_date_timestamp), channel
order by year(bill_date_timestamp), month(bill_date_timestamp), channel

select year(bill_date_timestamp) yr, month(bill_date_timestamp) mth, region , sum(Quantity) Total_quantity, sum(total_amount) total_sales 
from orders o join stores s on o.Delivered_StoreID = s.StoreID
group by year(bill_date_timestamp), month(bill_date_timestamp), region
order by year(bill_date_timestamp), month(bill_date_timestamp), region

select year(bill_date_timestamp) yr, month(bill_date_timestamp) mth, o.Delivered_StoreID ,sum(total_amount) total_sales 
from orders o join stores s on o.Delivered_StoreID = s.StoreID
group by year(bill_date_timestamp), month(bill_date_timestamp), o.Delivered_StoreID
order by year(bill_date_timestamp), month(bill_date_timestamp), o.Delivered_StoreID

select year(bill_date_timestamp) yr, month(bill_date_timestamp) mth, p.Category, sum(total_amount) total_sales 
from orders o join products p on o.product_id = p.product_id
group by year(bill_date_timestamp), month(bill_date_timestamp), p.Category
order by year(bill_date_timestamp), month(bill_date_timestamp), p.Category

select year(bill_date_timestamp) yr, month(bill_date_timestamp) mth, pd.payment_type , sum(Quantity) Total_quantity, sum(total_amount) total_sales 
from orders o join payments pd on o.order_id = pd.order_id
group by year(bill_date_timestamp), month(bill_date_timestamp), pd.payment_type
order by year(bill_date_timestamp), month(bill_date_timestamp), pd.payment_type


-- Popular categories/Popular Products by store, state, region. 

select * from (
select c.customer_state, o.product_id, sum(quantity) qnt, rank() over(partition by c.customer_state order by sum(quantity) desc) rnk 
from orders o join customers c on c.Custid = o.Customer_id
group by c.customer_state, o.product_id) tbl1
where rnk = 1 and tbl1.customer_state != 'Goa'

select * from (
select o.Delivered_StoreID, o.product_id, sum(quantity) qnt, rank() over(partition by delivered_storeid order by sum(quantity) desc) rnk 
from orders o join customers c on c.Custid = o.Customer_id
group by o.Delivered_StoreID, o.product_id) tbl1
where rnk = 1


-- List the top 10 most expensive products sorted by price and their contribution to sales

select product_id, round(sum(mrp * quantity),2) amt, round(sum(mrp * quantity) * 100.00/(select sum(total_amount) from orders),2) contri_sales from orders 
where product_id in 
(select top 10 product_id from orders
order by mrp desc)
group by product_id
order by amt desc


-- Which product appeared in the transactions?

select order_id, STRING_AGG(p.product_id, '-') prod_in_order from orders o join products p on o.product_id = p.product_id
group by order_id


-- Top 10-performing & worst 10 performance stores in terms of sales

select top 10 delivered_storeid, sum(total_amount) total_sales from orders
group by delivered_storeid
order by sum(total_amount) desc

select top 10 delivered_storeid, sum(total_amount) total_Sales from orders
group by delivered_storeid
order by sum(total_amount) asc


-- Question 2
-- Divide the customers into groups based on Recency, Frequency, and Monetary (RFM Segmentation) 
-- Divide the customers into Premium, Gold, Silver, Standard customers and understand the behaviour 
-- of each segment of customers


select segment, COUNT(customer_id) AS customer_count, AVG(recency) AS avg_recency, AVG(frequency) AS avg_frequency,
round(sum(monetary),2) AS total_monetary from 
(select *, (recency_score + frequency_score + monetary_score) AS rfm_score,
CASE WHEN (recency_score + frequency_score + monetary_score) >= 10 THEN 'Premium'
WHEN (recency_score + frequency_score + monetary_score) >= 7 THEN 'Gold'
WHEN (recency_score + frequency_score + monetary_score) >= 4 THEN 'Silver'
ELSE 'Standard' END AS segment from
(select *, NTILE(4) OVER (ORDER BY recency desc) AS recency_score, NTILE(4) OVER (ORDER BY frequency asc) AS frequency_score, 
NTILE(4) OVER (ORDER BY monetary asc) AS monetary_score from
(SELECT customer_id, DATEDIFF(day, MAX(Bill_date_timestamp), (select max(Bill_date_timestamp) from orders)) AS recency, 
COUNT(order_id) AS frequency, SUM(Total_Amount) AS monetary
FROM  orders
GROUP BY customer_id) tbl1) tbl2) tbl3
Group by segment;



-- Find out the number of customers who purchased in all the channels and find the key metrics.

select count(customer_id), sum(sales) from(
select customer_id, count(Channel) chnl, sum(Total_Amount) sales from orders
group by customer_id
having count(channel) = 4) tbl1;


-- Understand the behavior of one time buyers and repeat buyers
-- one time
select count(o.customer_id) cnt_cust, sum(Total_Amount) one_time_cust_sales from orders o join (
select customer_id, count(order_id) ord_cnt from orders
group by Customer_id
having count(distinct order_id) = 1) b
on o.Customer_id = b.Customer_id

-- repeat
select count(distinct o.customer_id) cnt_cust, sum(Total_Amount) one_time_cust_sales from orders o join (
select customer_id, count(order_id) ord_cnt from orders
group by Customer_id
having count(distinct order_id) > 1) b
on o.Customer_id = b.Customer_id

-- Understand the behavior of discount seekers & non discount seekers

select count(distinct customer_id) non_dis_seeker, sum(total_amount) sales from orders 
where discount > 0

select count(distinct customer_id) dis_seeker, sum(total_amount) sales from orders 
where discount = 0


-- Understand preferences of customers (preferred channel, Preferred payment method, preferred store, 
-- discount preference, preferred categories etc.)

select Channel, chn_cnt from (
select *, rank() over(partition by channel order by chn_cnt desc) rnk from(
select channel, count(order_id) chn_cnt from orders
group by Channel) tbl1) tbl2
where rnk = 1


-- customer based on revenue

select segment, round(sum(Total_Amount),2) sales from
(select customer_id, Total_Amount,  NTILE(4) OVER (ORDER BY total_amount desc) segment 
from orders) tbl
group by segment;



-- Question 3
-- Cross-Selling (Which products are selling together) 

WITH ProductPairs AS (
SELECT od1.product_id AS product1_id, od2.product_id AS product2_id, COUNT(*) AS times_bought_together
FROM orders od1 JOIN orders od2 ON od1.order_id = od2.order_id
WHERE od1.product_id < od2.product_id
GROUP BY od1.product_id, od2.product_id)

SELECT top 10 p1.product_id AS product1_name, p2.product_id AS product2_name,
pp.times_bought_together FROM ProductPairs pp JOIN products p1 ON pp.product1_id = p1.product_id
JOIN products p2 ON pp.product2_id = p2.product_id
where p2.product_id is not null
ORDER BY pp.times_bought_together DESC



-- Question 4
-- Total Sales & Percentage of sales by category (Perform Pareto Analysis)

select p.category , sum(total_amount) total_sales, 
sum(total_amount) * 100.00/(select sum(total_amount) from orders) percent_of_total
from orders o join products p on o.product_id = p.product_id
group by category
order by percent_of_total desc


-- Category Penetration Analysis by month on month (Category Penetration = number of orders containing the category/number of orders)

WITH OrdersPerMonth AS (
SELECT DATEPART(YEAR, Bill_date_timestamp) AS order_year, DATEPART(MONTH, Bill_date_timestamp) AS order_month,
COUNT(DISTINCT Bill_date_timestamp) AS total_orders FROM orders
GROUP BY DATEPART(YEAR, Bill_date_timestamp), DATEPART(MONTH, Bill_date_timestamp)
),
CategoryOrdersPerMonth AS (
SELECT DATEPART(YEAR, Bill_date_timestamp) AS order_year, DATEPART(MONTH, Bill_date_timestamp) AS order_month,
p.Category cat, COUNT(DISTINCT o.order_id) AS category_orders
FROM orders o JOIN products p ON o.product_id = p.product_id
GROUP BY DATEPART(YEAR, Bill_date_timestamp), DATEPART(MONTH, Bill_date_timestamp), p.Category
)
SELECT c.order_year, c.order_month, c.cat, c.category_orders, o.total_orders,
CAST(c.category_orders AS FLOAT) / o.total_orders AS category_penetration
FROM CategoryOrdersPerMonth c JOIN OrdersPerMonth o ON c.order_year = o.order_year AND c.order_month = o.order_month
ORDER BY c.order_year, c.order_month, c.cat;


-- Cross Category Analysis by month on Month (In Every Bill, how many categories shopped. Need to calculate average number of categories shopped in each bill by Region, By State etc


select order_id, count(distinct(category)) cnt from orders o join products p
on o.product_id = p.product_id
group by order_id
order by cnt desc


-- Most popular category during first purchase of customer

WITH FirstPurchases AS (SELECT o.customer_id, MIN(o.Bill_date_timestamp) AS first_purchase_date
FROM orders o
GROUP BY o.customer_id
),
FirstPurchaseOrders AS (
SELECT fp.customer_id, o.order_id, o.Bill_date_timestamp
FROM FirstPurchases fp JOIN orders o ON fp.customer_id = o.customer_id AND fp.first_purchase_date = o.Bill_date_timestamp
),
FirstPurchaseCategories AS (
SELECT fpo.customer_id, p.Category
FROM FirstPurchaseOrders fpo JOIN orders od ON fpo.order_id = od.order_id
JOIN products p ON od.product_id = p.product_id 
)
SELECT fpc.Category, COUNT(DISTINCT fpc.customer_id) AS customer_count
FROM FirstPurchaseCategories fpc
GROUP BY fpc.Category
ORDER BY customer_count DESC


-- Question 5
-- Which categories (top 10) are maximum rated & minimum rated and average rating score? 
-- Average rating by location, store, product, category, month, etc.


select c.customer_city, sum(total_amount) sales from orders o join customers c on o.Customer_id = c.Custid
group by c.customer_city
order by sales desc

select Category, count(Customer_Satisfaction_Score) max_rated from orders o 
join ratings r on o.order_id = r.order_id 
join products p on o.product_id = p.product_id
group by Category
order by max_rated desc

-- Average rating by category

select Category, avg(Customer_Satisfaction_Score) avg_rateing from orders o 
join ratings r on o.order_id = r.order_id 
join products p on o.product_id = p.product_id
group by Category

-- Average rating by store

select Delivered_StoreID, avg(Customer_Satisfaction_Score) avg_rateing from orders o 
join ratings r on o.order_id = r.order_id 
join products p on o.product_id = p.product_id
group by Delivered_StoreID

-- Average rating by location

select c.customer_state, avg(Customer_Satisfaction_Score) avg_rateing from orders o 
join ratings r on o.order_id = r.order_id 
join products p on o.product_id = p.product_id
join customers c on o.Customer_id = c.Custid
group by c.customer_state



-- Question 7

-- sales by month

select year(bill_date_timestamp) yr, month(bill_date_timestamp) mth ,round(sum(total_amount),2) total_sales from orders
group by year(bill_date_timestamp), month(bill_date_timestamp)
order by year(bill_date_timestamp), month(bill_date_timestamp)

-- Which months have had the highest sales, what is the sales amount and contribution in percentage?


select year(Bill_date_timestamp) yr, MONTH(Bill_date_timestamp) mth, sum(Total_Amount) sales_amt,
sum(Total_Amount)*100.00/(select sum(Total_Amount) from orders) contri_percent from orders
group by  year(Bill_date_timestamp), MONTH(Bill_date_timestamp)
order by sales_amt desc

-- Which months have had the least sales, what is the sales amount and contribution in percentage?  

select year(Bill_date_timestamp) yr, MONTH(Bill_date_timestamp) mth, sum(Total_Amount) sales_amt,
sum(Total_Amount)*100.00/(select sum(Total_Amount) from orders) contri_percent from orders
group by  year(Bill_date_timestamp), MONTH(Bill_date_timestamp)
order by sales_amt asc

-- Total Sales by Week of the Day

SELECT DATENAME(WEEKDAY, Bill_date_timestamp) AS day_of_week, SUM(Total_Amount) AS total_sales
FROM orders
GROUP BY DATENAME(WEEKDAY, Bill_date_timestamp), DATEPART(WEEKDAY, Bill_date_timestamp)
ORDER BY DATEPART(WEEKDAY, Bill_date_timestamp)


-- Sales by weekdays vs weekends

SELECT CASE WHEN DATEPART(WEEKDAY, Bill_date_timestamp) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
SUM(Total_Amount) AS total_sales
FROM orders
GROUP BY CASE WHEN DATEPART(WEEKDAY, Bill_date_timestamp) IN (1, 7) THEN 'Weekend'ELSE 'Weekday' END
ORDER BY day_type;
