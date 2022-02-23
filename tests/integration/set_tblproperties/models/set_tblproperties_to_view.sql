{{ config(
    tblproperties={
      'tblproperties_to_view' : 'true'
    }
) }}

select * from {{ ref('set_tblproperties') }}
