{{config(materialized='table', file_format='parquet')}}

select * from values
    (1, 'Alice', '2022-01-01'),
    (2, 'Bob', '2022-01-02')
    t(id, name, date)
