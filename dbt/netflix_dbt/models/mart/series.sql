with stg_netflix_show as (

    select *
    from {{ ref('stg_netflix_show') }}

),

series as (
    
    select 
        id_show,
        titulo,
        classificacao,
        ano_lancamento
        
    from stg_netflix_show
    where tipo = 'TV Show'

)

select *
from series