-- World Layoffs Data Cleaning

SELECT * 
FROM layoffs;

-- KEY OBJECTIVES
-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null values or Blank values
-- 4. Remove any columns and rows

-- seperating the raw data and the clean data

create table layoffs_stagging
like layoffs;

select*
from layoffs_stagging
;

insert layoffs_stagging
select * 
from layoffs
;

-- 1. Removing any duplicates

select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_stagging
;

with duplicate_cte as 
(
select *,
row_number() over(
partition by company, location,
industry, total_laid_off, percentage_laid_off, 'date', stage,
country, funds_raised_millions) as row_num
from layoffs_stagging
)
select*
from duplicate_cte 
where row_num > 1
;

select*
from layoffs_stagging
where company = 'Casper'
;


CREATE TABLE `layoffs_stagging2` (
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


select *
from layoffs_stagging2
;

insert into layoffs_stagging2
select *,
row_number() over(
partition by company, location,
industry, total_laid_off, percentage_laid_off, 'date', stage,
country, funds_raised_millions) as row_num
from layoffs_stagging
;

delete 
from layoffs_stagging2
where row_num > 1
;

select *
from layoffs_stagging2
where company like 'bally%'
;


-- 2. Standardizing data

select *
from layoffs_stagging2
;

-- 2.1 removing excess spaces in "industry"

select distinct (company)
from layoffs_stagging2
;

select company, trim(company)
from layoffs_stagging2
;

update layoffs_stagging2
set company = trim(company);

select distinct industry
from layoffs_stagging2
order by 1
;
select industry
from layoffs_stagging2
;
-- 2.2 identifying that "crypto" has a duplicates (im gonna change all into 'Crypto')

select *
from layoffs_stagging2
where industry like 'Crypto'
;

update layoffs_stagging2
set industry = 'Crypto'
where industry like 'Crypto%';

-- 2.3 identifying that 'united states' on a country side has a dot (.) 

select distinct country, trim(trailing '.' from country)
from layoffs_stagging2
ORDER BY 1
; 

update layoffs_stagging2
set country = trim(trailing '.' from country)
where country like 	'united states%'
;

select Country
from layoffs_stagging2
where country like 'united states%'
;
 
-- 2.4 formating date and changing the data type on date column (text) into a (date)

select `date`
from layoffs_stagging2
;

select `date`,
str_to_date (`date`, '%m/%d/%Y ')
from layoffs_stagging2
;

update layoffs_stagging2
set `date`= str_to_date (`date`, '%m/%d/%Y')
;

alter table layoffs_stagging2
modify column `date` date
;


-- 3. null or blank values

select *
from layoffs_stagging2
where total_laid_off is null
and percentage_laid_off is null
; 

update layoffs_stagging2
set industry = null
where industry = ''
;


select distinct industry
from layoffs_stagging2
;

select *
from layoffs_stagging2
where industry is null
or industry = ''
;

select *
from layoffs_stagging2
where company like 'Bally%'
;

select *
from layoffs_stagging2
where company = 'airbnb'
;

select t1.industry, t2.industry
from layoffs_stagging2 t1
join layoffs_stagging2 t2
	on t1.company = t2.company
where (t1.company is null or t1.industry = '')
and t2.industry is not null
;

update layoffs_stagging2 t1
join layoffs_stagging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.company is null 
and t2.industry is not null 
;

select *
from layoffs_stagging2
;

select *
from layoffs_stagging2
where total_laid_off is null
and percentage_laid_off is null
; 

-- 3.1 deleting  null values

delete 
from layoffs_stagging2
where total_laid_off is null
and percentage_laid_off is null
; 

select * 
from layoffs_stagging2
;

alter table layoffs_stagging2
drop column row_num
;