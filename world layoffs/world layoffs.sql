select *
from layoffs;

-- the purpose is to make working table that separate from original table

create table layoffs_stg
like layoffs;

insert layoffs_stg
select *
from layoffs;

-- looking for duplicate and deleting the duplicate data

select *, row_number () over(partition by company, location, industry, total_laid_off, 
percentage_laid_off , 'date', stage, country, funds_raised_millions) as row_numb
from layoffs_stg;

with duplicate_data as (select *, row_number () over(partition by company, location, industry, total_laid_off, 
percentage_laid_off , 'date', stage, country, funds_raised_millions) as row_numb
from layoffs_stg)
delete from duplicate_data
where row_numb > 1;

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

-- standardizing the data, industry data from various type like crypto, cryptocurrencirs to Crypto 

update layoffs_stg2
set industry = "Crypto"
where industry like "crypto%";

insert into layoffs_stg2
select *, row_number () over(partition by company, location, industry, total_laid_off, 
percentage_laid_off , 'date', stage, country, funds_raised_millions) as row_numb
from layoffs_stg;

-- continue to cleaning the data, trimming stage data that have some blanks

update layoffs_stg2
set stage = trim(stage);

-- filling up some industry data, based on company, if company name is same, but the other data is without industry, then we fill up from other data
-- using self join to match the company name with the industry

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

-- lastly we drop the unused column, and unusable data that doesn't have any values in it

alter table layoffs_stg2
drop column row_num;

delete 
from layoffs_stg2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_stg2;
