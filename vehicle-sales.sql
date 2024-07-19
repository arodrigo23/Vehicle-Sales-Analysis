/** This sql file contains examples of the queries used to explore, standardize, and clean the dataset.
	The dataset was imported into postgres using pgAdmin, then a staging table was created for any manipulations in order to preserve the original data	
**/

--Create table for vehicle sales dataset
create table sales (
	year int,
	make varchar,
	model varchar,
	trim varchar,
	body varchar,
	transmission varchar,
	vin varchar,
	state varchar,
	condition int,
	odometer int,
	color varchar,
	interior varchar,
	seller varchar,
	mmr int,
	selling_price int,
	sale_date varchar
);

--Create staging table
create table sales_staging (
	year int,
	make varchar,
	model varchar,
	trim varchar,
	body varchar,
	transmission varchar,
	vin varchar,
	state varchar,
	condition int,
	odometer int,
	color varchar,
	interior varchar,
	seller varchar,
	mmr int,
	selling_price int,
	sale_date varchar
);
insert into sales_staging
	select * from sales;
	
--Use pgAdmin to import data into tables

/* Explore data to identify anything that might be incorrect or missing. Investigation shows that records with 'Navitgation' in the "body" column have accidentally shifted the remaining columns over to the right by one column. Assume that 'Navitgation' should really be 'Navigation' and that this is supposed to be part of the "trim" column. */
  
/* Shift data into the correct columns, from left to right */
--Identify incorrect records
select *
from sales_staging
where body ilike '%Navitgation%';

--Change "trim" column to include 'Navigation'. 
--Note: all incorrect records had trim value 'SE PZEV w/Connectivity'
update sales_staging
set trim = 'SE PZEV w/Connectivity & Navigation'
where body = 'Navitgation';

--now update all other columns
update sales_staging
set body = transmission
where trim = 'SE PZEV w/Connectivity & Navigation';

update sales_staging
set transmission = vin
where trim = 'SE PZEV w/Connectivity & Navigation';

update sales_staging
set vin = state
where trim = 'SE PZEV w/Connectivity & Navigation';

update sales_staging
set state = NULL
where trim = 'SE PZEV w/Connectivity & Navigation';

update sales_staging
set condition = odometer
where trim = 'SE PZEV w/Connectivity & Navigation';

update sales_staging
set odometer = cast(color as integer)
where trim = 'SE PZEV w/Connectivity & Navigation';

update sales_staging
set color = interior
where trim = 'SE PZEV w/Connectivity & Navigation';

update sales_staging
set interior = seller
where trim = 'SE PZEV w/Connectivity & Navigation';

update sales_staging
set seller = cast(mmr as varchar)
where trim = 'SE PZEV w/Connectivity & Navigation';

update sales_staging
set mmr = selling_price
where trim = 'SE PZEV w/Connectivity & Navigation';

update sales_staging
set selling_price = cast(sale_date as integer)
where trim = 'SE PZEV w/Connectivity & Navigation';

--Since data was shifted rightward, we no longer have the sales_date data since this is the last column; set these to null
update sales_staging
set sale_date = NULL
where trim = 'SE PZEV w/Connectivity & Navigation';

--Verify updates are correct
select * 
from sales_staging
where trim = 'SE PZEV w/Connectivity & Navigation';

/* Handle null and blank values */

--Noticed some records have a dash listed under "color", let's make these null
--Identify records with a dash listed for color
select * 
from sales_staging
where length(color) < 2;

select color, length(color), count(*)
from sales_staging
group by color
order by length(color);

--Update staging table to make these records null
update sales_staging
set color = NULL
where length(color) = 1;

--Now do the same for "interior" which lists color of the car's interior
select *
from sales_staging
where length(interior) = 1;

select interior, length(interior), count(*)
from sales_staging
group by interior
order by length(interior);

update sales_staging
set interior = NULL
where length(interior) = 1;

--Find records with blank or null values for make and model
--If cannot insert values based on investigation of dataset, delete these records
select *
from sales_staging
where (make is null or make = '')
	   and (model is null or model = '');

delete from sales_staging
where make is null and model is null;

/* Standardize data */

-- Investigate fields and set values to uppercase where appropriate

--Example: Investigate "make" field
select
	make, upper(make)
from sales_staging;

select make, count(*)
from sales_staging
group by 1
order by 1; --see count of all makes

with upper_cte as (
	select 
		upper(make) as make_upper,
		*
	from sales_staging
)
select make_upper, count(*)
from upper_cte
group by 1
order by 1;  --count decreases when make is made uppercase; good opportunity for standardization

--Update table with standardized "make" column
update sales_staging
set make = upper(make);

--Verify update
select make, count(*)
from sales_staging
group by 1
order by 1;

--Example: standardize minor differences in make (e.g.: there is FORD, FORD TK, and FORD TRUCK; change them all to FORD)
update sales_staging
set make = 'FORD'
where make like 'FORD%';

--Example of investigating and standardizing Mazda models
select make, model, count(*)
from sales_staging
where make = 'MAZDA'
group by 1, 2
order by 1, 2;

update sales_staging
set model = 'RX8'
where make = 'MAZDA' and model = 'RX-8';

/* Invesitage validity of field values */

--Example: 
select vin
from sales_staging
where length(vin) != 17;

/* Investigate any duplicate records */

--Identify duplicates
with cte as (
	select 
		row_number() over(partition by 
			year,
			make, 
		  model,
		  trim,
		  body,
		  transmission,
		  vin,
		  state,
		  condition,
		  odometer,
		  color,
		  interior,
		  seller, 
		  mmr,
		  selling_price,
		  sale_date
		) as row_num,
		*
	from sales_staging
)
select *
from cte
where row_num > 1; --no duplicates found

/* Clean sale_date column. Example of current sale_date: 'Tue Dec 16 2014 12:30:00 GMT-0800 (PST)' */

select
	sale_date, 
	trim(right(sale_date, 4), ')') as timezone,
	to_date(substring(sale_date, 5, 11), 'Mon DD YYYY') || ' ' || 
		substring(sale_date, 17, 8) || ' ' || 
		trim(right(sale_date, 4), ')') as datetimezone,
	to_date(substring(sale_date, 5, 11), 'Mon DD YYYY') as sale_date_clean,
	((to_date(substring(sale_date, 5, 11), 'Mon DD YYYY') || ' ' || 
		substring(sale_date, 17, 8) || ' ' || 
		trim(right(sale_date, 4), ')'))::timestamp AT TIME ZONE (trim(right(sale_date, 4), ')'))) 
		AT TIME ZONE 'UTC' AS sale_datetime_clean
from sales_staging
where sale_date is not null;
	
alter table sales_staging
	add column sale_date_clean,
	add column sale_datetime_clean;
	
update sales_staging
	set sale_date_clean = to_date(substring(sale_date, 5, 11), 'Mon DD YYYY')
	where sale_date is not null;
	
update sales_staging
	set sale_datetime_clean = ((to_date(substring(sale_date, 5, 11), 'Mon DD YYYY') || ' ' || 
		substring(sale_date, 17, 8) || ' ' || 
		trim(right(sale_date, 4), ')'))::timestamp AT TIME ZONE (trim(right(sale_date, 4), ')'))) 
		AT TIME ZONE 'UTC'
	where sale_date is not null;

/* Add new columns to staging table for manufacturer, region of manufacture, and vehicle class (mass-market, luxury, or sport) based on research */

--Assign manufacturer to each make
alter table sales_staging
	add column manuf;

update sales_staging
	set manuf = case make
		when 'ACURA' then 'HONDA'
		when 'AUDI' then 'VOLKSWAGEN GROUP'
		when 'BENTLEY' then 'VOLKSWAGEN GROUP'
		when 'BMW' then 'BMW'
		when 'BUICK' then 'GM'
		when 'CADILLAC' then 'GM'
		when 'CHEVROLET' then 'GM'
		when 'CHRYSLER' then 'FIAT CHRYSLER'
		when 'DAEWOO' then 'GM'
		when 'DODGE' then 'FIAT CHRYSLER'
		when 'DOT' then 'DOT'
		when 'FERRARI' then 'FIAT CHRYSLER'
		when 'FIAT' then 'FIAT CHRYSLER'
		when 'FISKER' then 'FISKER'
		when 'FORD' then 'FORD'
		when 'GEO' then 'GM'
		when 'GMC' then 'GM'
		when 'HONDA' then 'HONDA'
		when 'HUMMER' then 'GM'
		when 'HYUNDAI' then 'HYUNDAI MOTOR GROUP'
		when 'INFINITI' then 'NISSAN'
		when 'ISUZU' then 'ISUZU'
		when 'JAGUAR' then 'TATA MOTORS'
		when 'JEEP' then 'FIAT CHRYSLER'
		when 'KIA' then 'HYUNDAI MOTOR GROUP'
		when 'LAMBORGHINI' then 'VOLKSWAGEN GROUP'
		when 'LAND ROVER' then 'TATA MOTORS'
		when 'LEXUS' then 'TOYOTA'
		when 'LINCOLN' then 'FORD'
		when 'MASERATI' then 'FIAT CHRYSLER'
		when 'MAZDA' then 'MAZDA'
		when 'MERCEDES-BENZ' then 'DAIMLER'
		when 'MERCURY' then 'FORD'
		when 'MINI' then 'BMW'
		when 'MITSUBISHI' then 'MITSUBISHI'
		when 'NISSAN' then 'NISSAN'
		when 'OLDSMOBILE' then 'GM'
		when 'PLYMOUTH' then 'FIAT CHRYSLER'
		when 'PONTIAC' then 'GM'
		when 'PORSCHE' then 'VOLKSWAGEN GROUP'
		when 'RAM' then 'FIAT CHRYSLER'
		when 'ROLLS-ROYCE' then 'BMW'
		when 'SAAB' then 'SPYKER CARS'
		when 'SATURN' then 'GM'
		when 'SCION' then 'TOYOTA'
		when 'SMART' then 'DAIMLER'
		when 'SUBARU' then 'SUBARU'
		when 'SUZUKI' then 'SUZUKI'
		when 'TESLA' then 'TESLA'
		when 'TOYOTA' then 'TOYOTA'
		when 'VOLKSWAGEN' then 'VOLKSWAGEN GROUP'
		when 'VOLVO' then 'GEELY'
		else null
	end

-- Add column for manufacturer region: domestic or foreign
alter table sales_staging
	add column dom_for varchar;
	
update sales_staging
	set dom_for = case make
			when 'ACURA' then 'FOREIGN'
			when 'AIRSTREAM' then 'DOMESTIC'
			when 'ASTON MARTIN' then 'FOREIGN'
			when 'AUDI' then 'FOREIGN'
			when 'BENTLEY' then 'FOREIGN'
			when 'BMW' then 'FOREIGN'
			when 'BUICK' then 'DOMESTIC'
			when 'CADILLAC' then 'DOMESTIC'
			when 'CHEVROLET' then 'DOMESTIC'
			when 'CHRYSLER' then 'DOMESTIC'
			when 'DAEWOO' then 'FOREIGN'
			when 'DODGE' then 'DOMESTIC'
			when 'DOT' then 'FOREIGN'
			when 'FERRARI' then 'FOREIGN'
			when 'FIAT' then 'FOREIGN'
			when 'FISKER' then 'DOMESTIC'
			when 'FORD' then 'DOMESTIC'
			when 'GEO' then 'DOMESTIC'
			when 'GMC' then 'DOMESTIC'
			when 'HONDA' then 'FOREIGN'
			when 'HUMMER' then 'DOMESTIC'
			when 'HYUNDAI' then 'FOREIGN'
			when 'INFINITI' then 'FOREIGN'
			when 'ISUZU' then 'FOREIGN'
			when 'JAGUAR' then 'FOREIGN'
			when 'JEEP' then 'DOMESTIC'
			when 'KIA' then 'FOREIGN'
			when 'LAMBORGHINI' then 'FOREIGN'
			when 'LAND ROVER' then 'FOREIGN'
			when 'LEXUS' then 'FOREIGN'
			when 'LINCOLN' then 'DOMESTIC'
			when 'LOTUS' then'FOREIGN'
			when 'MASERATI' then 'FOREIGN'
			when 'MAZDA' then 'FOREIGN'
			when 'MERCEDES-BENZ' then 'FOREIGN'
			when 'MERCURY' then 'DOMESTIC'
			when 'MINI' then 'FOREIGN'
			when 'MITSUBISHI' then 'FOREIGN'
			when 'NISSAN' then 'FOREIGN'
			when 'OLDSMOBILE' then 'DOMESTIC'
			when 'PLYMOUTH' then 'DOMESTIC'
			when 'PONTIAC' then 'DOMESTIC'
			when 'PORSCHE' then 'FOREIGN'
			when 'RAM' then 'DOMESTIC'
			when 'ROLLS-ROYCE' then 'FOREIGN'
			when 'SAAB' then 'FOREIGN'
			when 'SATURN' then 'DOMESTIC'
			when 'SCION' then 'FOREIGN'
			when 'SMART' then 'FOREIGN'
			when 'SUBARU' then 'FOREIGN'
			when 'SUZUKI' then 'FOREIGN'
			when 'TESLA' then 'DOMESTIC'
			when 'TOYOTA' then 'FOREIGN'
			when 'VOLKSWAGEN' then 'FOREIGN'
			when 'VOLVO' then 'FOREIGN'
			else null
		end;
		
--Add column for vehicles that are part of the mass-market class
alter table sales_staging
	add column mass_market boolean;

update sales_staging
	set mass_market = case make
			when 'ACURA' then FALSE
			when 'ASTON MARTIN' then FALSE
			when 'AUDI' then FALSE
			when 'BENTLEY' then FALSE
			when 'BMW' then FALSE
			when 'BUICK' then TRUE
			when 'CADILLAC' then FALSE
			when 'CHEVROLET' then TRUE
			when 'CHRYSLER' then TRUE
			when 'DAEWOO' then TRUE
			when 'DODGE' then TRUE
			when 'DOT' then FALSE
			when 'FERRARI' then FALSE
			when 'FIAT' then TRUE
			when 'FISKER' then FALSE
			when 'FORD' then TRUE
			when 'GEO' then TRUE
			when 'GMC' then TRUE
			when 'HONDA' then TRUE
			when 'HUMMER' then FALSE
			when 'HYUNDAI' then TRUE
			when 'INFINITI' then FALSE
			when 'ISUZU' then TRUE
			when 'JAGUAR' then FALSE
			when 'JEEP' then TRUE
			when 'KIA' then TRUE
			when 'LAMBORGHINI' then FALSE
			when 'LAND ROVER' then FALSE
			when 'LEXUS' then FALSE
			when 'LINCOLN' then FALSE
			when 'LOTUS' then FALSE
			when 'MASERATI' then FALSE
			when 'MAZDA' then FALSE
			when 'MERCEDES-BENZ' then FALSE
			when 'MERCURY' then FALSE
			when 'MINI' then TRUE
			when 'MITSUBISHI' then TRUE
			when 'NISSAN' then TRUE
			when 'OLDSMOBILE' then TRUE
			when 'PLYMOUTH' then TRUE
			when 'PONTIAC' then TRUE
			when 'PORSCHE' then FALSE
			when 'RAM' then TRUE
			when 'ROLLS-ROYCE' then FALSE
			when 'SAAB' then FALSE
			when 'SATURN' then TRUE
			when 'SCION' then TRUE
			when 'SMART' then TRUE
			when 'SUBARU' then TRUE
			when 'SUZUKI' then TRUE
			when 'TESLA' then TRUE
			when 'TOYOTA' then TRUE
			when 'VOLKSWAGEN' then TRUE
			when 'VOLVO' then FALSE
			else null
		end

--Add column for vehicles that are part of the luxury class
alter table sales_staging
	add column luxury boolean;

update sales_staging
	set luxury = case make
			when 'ACURA' then TRUE
			when 'ASTON MARTIN' then TRUE
			when 'AUDI' then TRUE
			when 'BENTLEY' then TRUE
			when 'BMW' then TRUE
			when 'BUICK' then FALSE
			when 'CADILLAC' then TRUE
			when 'CHEVROLET' then FALSE
			when 'CHRYSLER' then FALSE
			when 'DAEWOO' then FALSE
			when 'DODGE' then FALSE
			when 'FERRARI' then TRUE
			when 'FIAT' then FALSE
			when 'FISKER' then TRUE
			when 'FORD' then FALSE
			when 'GEO' then FALSE
			when 'GMC' then FALSE
			when 'HONDA' then FALSE
			when 'HUMMER' then FALSE
			when 'HYUNDAI' then FALSE
			when 'INFINITI' then TRUE
			when 'ISUZU' then FALSE
			when 'JAGUAR' then TRUE
			when 'JEEP' then FALSE
			when 'KIA' then FALSE
			when 'LAMBORGHINI' then TRUE
			when 'LAND ROVER' then TRUE
			when 'LEXUS' then TRUE
			when 'LINCOLN' then TRUE
			when 'LOTUS' then FALSE
			when 'MASERATI' then TRUE
			when 'MAZDA' then TRUE
			when 'MERCEDES-BENZ' then TRUE
			when 'MERCURY' then FALSE
			when 'MINI' then FALSE
			when 'MITSUBISHI' then FALSE
			when 'NISSAN' then FALSE
			when 'OLDSMOBILE' then FALSE
			when 'PLYMOUTH' then FALSE
			when 'PONTIAC' then FALSE
			when 'PORSCHE' then TRUE
			when 'RAM' then FALSE
			when 'ROLLS-ROYCE' then TRUE
			when 'SAAB' then FALSE
			when 'SATURN' then FALSE
			when 'SCION' then FALSE
			when 'SMART' then FALSE
			when 'SUBARU' then FALSE
			when 'SUZUKI' then FALSE
			when 'TESLA' then TRUE
			when 'TOYOTA' then FALSE
			when 'VOLKSWAGEN' then FALSE
			when 'VOLVO' then TRUE
			else null
		end

--Add column for vehicles that are part of the sport class
alter table sales_staging
	add column sport boolean;

update sales_staging
	set sport = case make
			when 'ACURA' then FALSE
			when 'ASTON MARTIN' then TRUE
			when 'AUDI' then FALSE
			when 'BENTLEY' then FALSE
			when 'BMW' then TRUE
			when 'BUICK' then FALSE
			when 'CADILLAC' then FALSE
			when 'CHEVROLET' then FALSE
			when 'CHRYSLER' then FALSE
			when 'DAEWOO' then FALSE
			when 'DODGE' then FALSE
			when 'DOT' then FALSE
			when 'FERRARI' then TRUE
			when 'FIAT' then FALSE
			when 'FISKER' then FALSE
			when 'FORD' then FALSE
			when 'GEO' then FALSE
			when 'GMC' then FALSE
			when 'HONDA' then FALSE
			when 'HUMMER' then TRUE
			when 'HYUNDAI' then FALSE
			when 'INFINITI' then FALSE
			when 'ISUZU' then FALSE
			when 'JAGUAR' then TRUE
			when 'JEEP' then FALSE
			when 'KIA' then FALSE
			when 'LAMBORGHINI' then TRUE
			when 'LAND ROVER' then TRUE
			when 'LEXUS' then FALSE
			when 'LINCOLN' then FALSE
			when 'LOTUS' then TRUE
			when 'MASERATI' then TRUE
			when 'MAZDA' then TRUE
			when 'MERCEDES-BENZ' then FALSE
			when 'MERCURY' then FALSE
			when 'MINI' then FALSE
			when 'MITSUBISHI' then FALSE
			when 'NISSAN' then FALSE
			when 'OLDSMOBILE' then FALSE
			when 'PLYMOUTH' then FALSE
			when 'PONTIAC' then FALSE
			when 'PORSCHE' then TRUE
			when 'RAM' then FALSE
			when 'ROLLS-ROYCE' then FALSE
			when 'SAAB' then FALSE
			when 'SATURN' then FALSE
			when 'SCION' then FALSE
			when 'SMART' then FALSE
			when 'SUBARU' then FALSE
			when 'SUZUKI' then FALSE
			when 'TESLA' then FALSE
			when 'TOYOTA' then FALSE
			when 'VOLKSWAGEN' then FALSE
			when 'VOLVO' then FALSE
			else null
		end;
		
/* Dataset has no unique identifier column; add row_number field */

--Create new staging table with row_num column
create table sales_staging2 (
	row_num int,
	year int,
	make varchar,
	model varchar,
	trim varchar,
	body varchar,
	transmission varchar,
	vin varchar,
	state varchar,
	condition int,
	odometer int,
	color varchar,
	interior varchar,
	seller varchar,
	mmr int,
	selling_price int,
	sale_date varchar,
	sale_date_clean date,
	sale_datetime_clean timestamp,
	manuf varchar,
	dom_for varchar,
	mass_market boolean,
	luxury boolean,
	sport boolean
);

insert into sales_staging2
	select 
		row_number() over(partition by 
			year,
			make,
			model,
			trim,
			body,
			transmission,
			vin,
			state,
			condition,
			odometer,
			color,
			interior,
			seller,
			mmr,
			selling_price,
			sale_date
		) as row_num,
		* 
	from sales_staging;