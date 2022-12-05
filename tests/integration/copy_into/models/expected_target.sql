{{config(materialized='table')}}

select * from values
    (0, 'Zero', '2022-01-01'),
    (1, 'Alice', '2022-01-01'),
    (2, 'Bob', '2022-01-02')
    as t(id, name, date)
