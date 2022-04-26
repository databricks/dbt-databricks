{{ config(materialized='table') }}

select * from values
  (1, 'Alice', '2022-01-01'),
  (2, 'Bob', '2022-02-01'),
  (3, 'Cathy', '2022-03-01')
t(id, name, date);
