create database Project2;
use Project2;

show tables;

select count(*) from layoffs; -- We have tota 2361 columns

select * 
from layoffs;
# We ca see null values in the poercentage_laid_off, funds_raised_millions, total_laid_off columns

# Steps
-- 1. Remove the duplicates values
-- 2. Remove any columns if necessary
-- 3. Deal with null or blank values
-- 4. Standardize the data

# Before changing main data we will clone the data
create table layoff
like layoffs;

select * from layoff;
 
insert layoff
select * 
from layoffs; # data inserted

select * from layoff;
# Step 1.

# Duplicate value
select * from layoff;

select *, count(*) count from layoff
group by 
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
having count>1;
-- There is duplicate values
with ranked_data as (
    select 
        company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
        row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions order by (select null)) as rn
    from layoff
)
delete from layoff
where (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) in (
    select company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    from ranked_data
    where rn > 1
);

-- Duplicate values has been removed

# Step 2 

select * from layoff;
-- All the columns are important so we will not remove any columns

# Step 3

# Null or duplicate values
select * from layoff;

select * from layoff
where company is null 
  or location is null 
  or industry is null 
  or total_laid_off is null 
  or percentage_laid_off is null 
  or `date` is null 
  or stage is null 
  or country is null 
  or funds_raised_millions is null;
-- we can a lot of null values here

# We don't know how many employee are laid off so I will replace the value with 0 for int col and 'Unknown' for text or date columns.

update layoff
set total_laid_off = coalesce(total_laid_off,0),
percentage_laid_off = coalesce(percentage_laid_off,0),
funds_raised_millions = coalesce(funds_raised_millions,0),
stage = coalesce(stage,'Unknown'),
`date` = coalesce(`date`,'Unknown'),
industry = coalesce(industry,'Unknown')
where total_laid_off is null
or percentage_laid_off is null
or funds_raised_millions is null
or stage is null
or `date` is null
or industry is null;
-- Total 1274 value has been changed
select * from layoff
where company is null 
  or location is null 
  or industry is null 
  or total_laid_off is null 
  or percentage_laid_off is null 
  or `date` is null 
  or stage is null 
  or country is null 
  or funds_raised_millions is null;


# Step 4 

# Standardize the values

desc layoff;

-- We only have to standardize the 'total_laid_off' and 'funds_raised_millions' column

# 'total_laid_off' column
create temporary table stats as 
select avg(total_laid_off) mean_v,stddev(total_laid_off) std_v from layoff;

select * from stats; 

alter table layoff
modify total_laid_off float;

update layoff l
join stats s 
on 1=1
set l.total_laid_off = (l.total_laid_off - mean_v) / std_v;

select * from layoff;

drop table stats;

# 'funds_raised_millions' column
create temporary table stats as 
select avg(funds_raised_millions) mean_v,stddev(funds_raised_millions) std_v from layoff;
select * from stats; 

alter table layoff
modify funds_raised_millions float;

update layoff l
join stats s 
on 1=1
set l.funds_raised_millions = (l.funds_raised_millions - mean_v) / std_v;

select * from layoff;

drop table stats;

# Data Cleaning is complete.