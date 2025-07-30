select *
from layoffs;

-- 1. We make a separate table so if we make any mistake, we have a back up

create table layoffs_stg
like layoffs;

insert layoffs_stg
select *
from layoffs;

-- then we looking for duplicate and deleting the duplicate data

select *, row_number () over(partition by company, location, industry, total_laid_off, 
percentage_laid_off , 'date', stage, country, funds_raised_millions) as row_num
from layoffs_stg;

CREATE TABLE `layoffs_stg2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_stg2
select *, row_number () over(partition by company, location, industry, total_laid_off, 
percentage_laid_off , 'date', stage, country, funds_raised_millions) as row_num
from layoffs_stg;

delete 
from layoffs_stg2
where row_num>1;

-- then we will standardize the data, we can check in the industry data 
-- there are a lot of variances of crypto so we will standardize from various type like crypto, cryptocurrencirs to just Crypto 

select distinct industry
from layoffs_stg2;

update layoffs_stg2
set industry = "Crypto"
where industry like "crypto%";

-- we will continue to cleaning the data, trimming stage data that have some blanks

update layoffs_stg2
set stage = trim(stage);

-- then we will fill up industry data, some row doesn't have industry data even though in the other row with same company name has
-- so we will fill it based on company, if company name is same, but the other data is without industry, then we fill up from other data
-- using self join to match the company name with the industry
-- we will first set all the blanks data to null, so we can work with the data

update layoffs_stg2
set industry = null
where industry ='';

select t1.company, t1.industry,t2.industry
from layoffs_stg2 t1
join layoffs_stg2 t2
on t1.company = t2.company
where (t1.industry is null or t1.industry='')
and t2.industry is not null;

update layoffs_stg2 t1
join layoffs_stg2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry='')
and t2.industry is not null;

-- And the last step of cleaning, we will drop any columns and row that we can't work with
-- which in this case is the row_num column we use for duplicate and some rows that
-- don't have both total_laid_off and percentage_laid_off data 

alter table layoffs_stg2
drop column row_num;

delete 
from layoffs_stg2
where total_laid_off is null
and percentage_laid_off is null;

-- And now our data is ready!

select *
from layoffs_stg2;
