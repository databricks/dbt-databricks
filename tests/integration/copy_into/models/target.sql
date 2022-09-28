{{config(materialized='table')}}

select * from values
	(0, 'Zero', '2022-01-01') as t(id, name, date)
