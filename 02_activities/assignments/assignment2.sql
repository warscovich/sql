/* ASSIGNMENT 2 */
--Participant Name: Julian Bueno

/* SECTION 2 */

-- COALESCE
/* 1. Our favourite manager wants a detailed long list of products, but is afraid of tables! 
We tell them, no problem! We can produce a list with all of the appropriate details. 

Using the following syntax you create our super cool and not at all needy manager a list:

SELECT 
product_name || ', ' || product_size|| ' (' || product_qty_type || ')'
FROM product

But wait! The product table has some bad data (a few NULL values). 
Find the NULLs and then using COALESCE, replace the NULL with a 
blank for the first problem, and 'unit' for the second problem. 

HINT: keep the syntax the same, but edited the correct components with the string. 
The `||` values concatenate the columns into strings. 
Edit the appropriate columns -- you're making two edits -- and the NULL rows will be fixed. 
All the other rows will remain the same.) */

SELECT  product_name || ', ' || coalesce(product_size,'')|| ' (' || coalesce(product_qty_type,'unit') || ')' as long_name
FROM product;



--Windowed Functions
/* 1. Write a query that selects from the customer_purchases table and numbers each customer’s  
visits to the farmer’s market (labeling each market date with a different number). 
Each customer’s first visit is labeled 1, second visit is labeled 2, etc. 

You can either display all rows in the customer_purchases table, with the counter changing on
each new market date for each customer, or select only the unique market dates per customer 
(without purchase details) and number those visits. 
HINT: One of these approaches uses ROW_NUMBER() and one uses DENSE_RANK(). */
select distinct market_date,customer_id, dense_rank() over (partition by customer_id order by market_date,customer_id) as visit
from customer_purchases;


/* 2. Reverse the numbering of the query from a part so each customer’s most recent visit is labeled 1, 
then write another query that uses this one as a subquery (or temp table) and filters the results to 
only the customer’s most recent visit. */

select market_date,customer_id from (select distinct market_date,customer_id, dense_rank() over (partition by customer_id order by market_date desc, customer_id) as visit
from customer_purchases) where visit = 1;


/* 3. Using a COUNT() window function, include a value along with each row of the 
customer_purchases table that indicates how many different times that customer has purchased that product_id. */
select distinct customer_id,product_id, count() over (partition by customer_id,product_id order by customer_id) as total_times_purchased
from customer_purchases order by customer_id;


-- String manipulations
/* 1. Some product names in the product table have descriptions like "Jar" or "Organic". 
These are separated from the product name with a hyphen. 
Create a column using SUBSTR (and a couple of other commands) that captures these, but is otherwise NULL. 
Remove any trailing or leading whitespaces. Don't just use a case statement for each product! 

| product_name               | description |
|----------------------------|-------------|
| Habanero Peppers - Organic | Organic     |

Hint: you might need to use INSTR(product_name,'-') to find the hyphens. INSTR will help split the column. */
select product_name,case when instr(product_name,'-') > 0 THEN description else null end 
from(
	select product_name,trim(substr(product_name,instr(product_name,'-')+1)) as description from product
);

-- UNION
/* 1. Using a UNION, write a query that displays the market dates with the highest and lowest total sales.

HINT: There are a possibly a few ways to do this query, but if you're struggling, try the following: 
1) Create a CTE/Temp Table to find sales values grouped dates; 
2) Create another CTE/Temp table with a rank windowed function on the previous query to create 
"best day" and "worst day"; 
3) Query the second temp table twice, once for the best day, once for the worst day, 
with a UNION binding them. */
j
create table temp.total_sales_market_day as
select market_date,
sum((quantity * cost_to_customer_per_qty ))as total_sale_day
from customer_purchases 
group by market_date order by total_sale_day;

select market_date,total_sale_day from(
	select market_date, 
	total_sale_day, 
	row_number() over (order by total_sale_day desc) as market_order
	from temp.total_sales_market_day
	union
	select market_date, 
	total_sale_day, 
	row_number() over (order by total_sale_day ) as market_order
	from temp.total_sales_market_day
)where market_order = 1;


/* SECTION 3 */

-- Cross Join
/*1. Suppose every vendor in the `vendor_inventory` table had 5 of each of their products to sell to **every** 
customer on record. How much money would each vendor make per product? 
Show this by vendor_name and product name, rather than using the IDs.

HINT: Be sure you select only relevant columns and rows. 
Remember, CROSS JOIN will explode your table rows, so CROSS JOIN should likely be a subquery. 
Think a bit about the row counts: how many distinct vendors, product names are there (x)?
How many customers are there (y). 
Before your final group by you should have the product of those two queries (x*y).  */

-- x = 8, y = 26 -> 208
--NOTE: Not using the discounted cost_to_customer_per_qty since it varies 
--and can't determine what is the quantity for discount

select product_name, vendor_name, sum(original_price * 5) as total_sell
from customer
cross join
(
	select distinct v.vendor_id,p.product_id, p.product_name,v.vendor_name,vi.original_price
	from vendor_inventory vi
	inner join product p on p.product_id = vi.product_id
	inner join vendor v on v.vendor_id = vi.vendor_id
	order by v.vendor_id,p.product_id
) group by vendor_name, product_name;



-- INSERT
/*1.  Create a new table "product_units". 
This table will contain only products where the `product_qty_type = 'unit'`. 
It should use all of the columns from the product table, as well as a new column for the `CURRENT_TIMESTAMP`.  
Name the timestamp column `snapshot_timestamp`. */
create table product_units as
select *,current_timestamp as snapshot_timestamp 
from product where product_qty_type = 'unit';


/*2. Using `INSERT`, add a new row to the product_units table (with an updated timestamp). 
This can be any product you desire (e.g. add another record for Apple Pie). */
insert into product_units 
values(24,'Blueberry pie','10"',3,'unit',current_timestamp);


-- DELETE
/* 1. Delete the older record for the whatever product you added. 

HINT: If you don't specify a WHERE clause, you are going to have a bad time.*/

-- Could be a simple delete with the Id but since is the older record...
delete from product_units 
where snapshot_timestamp in 
(select snapshot_timestamp from product_units order by snapshot_timestamp desc limit 1);

-- UPDATE
/* 1.We want to add the current_quantity to the product_units table. 
First, add a new column, current_quantity to the table using the following syntax.

ALTER TABLE product_units
ADD current_quantity INT;

Then, using UPDATE, change the current_quantity equal to the last quantity value from the vendor_inventory details.

HINT: This one is pretty hard. 
First, determine how to get the "last" quantity per product. 
Second, coalesce null values to 0 (if you don't have null values, figure out how to rearrange your query so you do.) 
Third, SET current_quantity = (...your select statement...), remembering that WHERE can only accommodate one column. 
Finally, make sure you have a WHERE statement to update the right row, 
	you'll need to use product_units.product_id to refer to the correct row within the product_units table. 
When you have all of these components, you can run the update statement. */

ALTER TABLE product_units
ADD current_quantity INT;

create table temp.latest_quantity_by_product_id as
select pu.product_id,coalesce(qp.quantity,0) as latest_quantity
from product_units pu
left join 
(
	select market_date, quantity, product_id, 
	row_number() over (partition by product_id order by market_date desc) as quantity_by_date_rank
	from vendor_inventory
) qp on qp.product_id = pu.product_id where qp.quantity_by_date_rank = 1 or qp.quantity_by_date_rank is null;

update product_units
set current_quantity = latest.latest_quantity
from temp.latest_quantity_by_product_id latest
where latest.product_id = product_units.product_id;



/*	select * from (select market_date, quantity, product_id, 
	row_number() over (partition by product_id order by market_date desc) as quantity_by_date_rank
	from vendor_inventory) order by quantity_by_date_rank;*/





