with stg_netflix_show as (

    select *
    from {{ ref('stg_netflix_show') }}

),

filmes as (
    
    select 
        id_show,
        titulo,
        classificacao,
        ano_lancamento
        
    from stg_netflix_show
    where tipo = 'Movie'

)

select *
from filmes