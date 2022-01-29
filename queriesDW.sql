-- DW Analysis -- 
-- Q1 -- 
-- Total sales of all products supplied by each supplier wrt quarter and month

Select sd.SupplierName, p.ProductID ,p.ProductName, dd.Quarter, dd.Month, sum(Total_Sale) as TotalSales from 
Transactions_Fact tf, Supplier_dim sd, Date_Dim dd, Product_Dim p
where tf.supplierID = sd.supplierID and tf.dateId = dd.DateId and
tf.ProductID= p.ProductID
group by sd.supplierName, p.ProductID, dd.Quarter, dd.Month WITH ROLLUP;

-- Q2 --
-- Total Sales sold by each store. Output Store wise then product wise under each store

Select s.StoreName,  p.ProductName,p.ProductID,su.supplierName,
c.customerID, sum(Total_Sale), tf.transactionID
from
Transactions_Fact tf, Store_dim s, Product_Dim p , supplier_dim su, customer_dim c
where tf.storeID = s.storeID and tf.ProductID = p.productID and tf.supplierID = su.SupplierID and
c.customerID = tf.customerID
group by s.storeid, p.ProductName WITH ROLLUP
order by s.StoreName, p.Productid;

-- Q3 --
-- Find 5 most popular products sold over the weekend
Select x.ProductName, x.TotalSold
from(
	select sum(quantity) as TotalSold, p.ProductName from transactions_fact tf, 
    product_dim p, date_dim d 
    where d.DayWeek IN ('Saturday', 'Sunday') and
    tf.dateID = d.dateID and tf.productID = p.productID 
    group by p.ProductID
    order by sum(quantity) desc
) as x
LIMIT 5;


-- Q4
-- Quarterly sales of each product for year 2016, each quarter must be a column

Select p.ProductName,
sum(case when d.quarter = 1 then tf.Total_Sale else 0 end) as Quarter_1,
sum(case when d.quarter = 2 then tf.Total_Sale else 0 end) as Quarter_2,
sum(case when d.quarter = 3 then tf.Total_Sale else 0 end) as Quarter_3,
sum(case when d.quarter = 4 then tf.Total_Sale else 0 end) as Quarter_4
from product_dim p, transactions_fact tf, date_dim d
where tf.ProductID = p.ProductID and tf.dateID = d.dateID
group by p.ProductID
order by p.ProductName;

-- Q5 -- 
-- Total sales of each product for the first and second half of the year 2016
-- along with its total yearly sales

Select p.productName,
sum(case when d.quarter = 1 or d.quarter=2 then tf.Total_Sale else 0 end) as FirstHalf,
sum(case when d.quarter = 3 or d.quarter=4 then tf.Total_Sale else 0 end) as SecondHalf,
sum(tf.Total_Sale) as Total_Yearly_Sales
from product_dim p, transactions_fact tf, date_dim d
where d.Year = 2016 and tf.ProductID=p.ProductID and 
d.DateID = tf.DateID 
group by p.ProductID
order by p.ProductName;



-- Q6 --
-- Anomaly in the data set
select transactionID, DateID,CustomerID from transactions_fact 
group by DateID, CustomerID order by DateID;

-- Q7 --
-- Materialized views are saved in the database and are NOT created as a View
drop table if exists STOREANALYSIS_MV;
Create table STOREANALYSIS_MV as
	Select tf.storeID as STORE_ID, tf.productID as PRODUCT_ID, sum(tf.Total_Sale) as STORE_TOTAL
	from transactions_fact tf
	group by tf.StoreID,tf.ProductID;
Select * from STOREANALYSIS_MV ;

