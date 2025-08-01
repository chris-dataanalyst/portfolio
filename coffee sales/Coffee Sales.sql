-- Coffee Sales Table from Vending Machine in Ukraine
-- Each Table represent different machine

select *
from index_1;

select *
from index_2;

-- First thing we do, we will check whether there is duplicate in either of table we have here.

select *, row_number()over(partition by date, datetime,cash_type,card,money,coffee_name) as row_num
from index_1;

select *, row_number()over(partition by date, datetime,cash_type,money,coffee_name) as row_num
from index_2;

-- we can see there is duplicate data in index_2 table, we will get rid of that later
-- we will merge these two table into one, so the visuzalization will be easier
-- so we will make a new table with new column that indicate which vending machine this sale from

select *, '1' as vending_machine
from index_1;

select *, '2' as vending_machine
from index_2;

CREATE TABLE `index_1stg` (
  `date` date,
  `datetime` text,
  `cash_type` text,
  `card` text,
  `money` double DEFAULT NULL,
  `coffee_name` text,
  `vending_machine` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into index_1stg
select * , '1' as vending_machine
from index_1;

select *
from index_1stg;

CREATE TABLE `index_2stg` (
  `date` date,
  `datetime` text,
  `cash_type` text,
  `card` text,
  `money` double DEFAULT NULL,
  `coffee_name` text,
  `vending_machine` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into index_2stg
select `date`, datetime, cash_type, ' ' as card, money, coffee_name, '2' as vending_machine, row_number()over(partition by date, datetime,cash_type,money,coffee_name) as row_num
from index_2;

select *
from index_2stg;

-- next, we will delete the duplicate data in the index_2 table, then we will drop the row_num column

delete from index_2stg
where row_num > 1;

alter table index_2stg
drop column row_num;

-- next we will combine these two table

CREATE TABLE `index_combine` (
  `date` date DEFAULT NULL,
  `datetime` text,
  `cash_type` text,
  `card` text,
  `money` double DEFAULT NULL,
  `coffee_name` text,
  `vending_machine` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into index_combine
select *
from index_1stg;

insert into index_combine
select *
from index_2stg;

-- since we already have standalone date column, we will use substring and change datetime column to time

select datetime, substring(datetime, 11, 9) as time
from index_combine;

update index_combine
set datetime = substring(datetime, 11, 9);

alter table index_combine
rename column datetime to `time`;

-- next we want to check if there is any data we need to standardize in coffee column

select distinct coffee_name
from index_combine;

-- I suspect the hot chocolate and cocoa are the same item, we will pull both value and check if the price is same

select *
from index_combine
where coffee_name = 'hot chocolate'
or coffee_name = 'cocoa';

-- the price is same, so it is the same item, so we will change Cocoa to chocolate

update index_combine
set coffee_name = 'Hot chocolate'
where coffee_name = 'cocoa';

select distinct coffee_name
from index_2;

-- we get way too much variance in this table, we will try to check the price as well to see if there is some same item

select *
from index_combine
where coffee_name like '%coffee%';

select *
from index_combine
where coffee_name like '%chocolate%';

update index_combine
set coffee_name = 'Chocolate with coffee'
where coffee_name = 'Coffee with chocolate';

update index_combine
set coffee_name = 'Hot Chocolate'
where coffee_name = 'Double Chocolate';

update index_combine
set coffee_name = 'Hot Chocolate'
where coffee_name = 'Chocolate';

-- So the variance that we can put into the same name is just coffee with chocolate to choholate with coffee, then some chocolate basic variance to
-- hot chocolate, next we will change the data type of time column to time

alter table index_combine
modify column time time;

-- we want to be able to pull how many return customer as well, so we will make it into a new table

select card, count(card)
from index_combine
group by card;

update index_combine
set card = ' '
where card = '';

update index_combine
set card = null
where card = ' ';

update index_combine
set card = right(card,4);

select *
from index_combine;

CREATE TABLE `card_purchase` (
  `card` text,
  `number_purchase` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into card_purchase
select card, count(card)
from index_combine
group by card;

select * 
from card_purchase;

-- alright now our table is set, we are ready to make a visualization