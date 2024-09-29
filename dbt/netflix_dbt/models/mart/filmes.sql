with stg_netflix_show as (

    select *
    from {{ ref('stg_netflix_show') }}

)