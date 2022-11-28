{{config(materialized='table')}}

select * from values
    (0, 'Zero', '2022-01-01'),
    (1, 'Alice', null),
    (2, 'Bob', null)
    as t(id, name, date)
