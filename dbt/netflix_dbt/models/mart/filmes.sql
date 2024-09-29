with stg_netflix_show as (

    select *
    from {{ ref('stg_netflix_show') }}

),

filmes as (
    
    select 
        id_show AS "ID Filme",
        titulo AS "Título do Filme",
        classificacao AS "Classificação",
        ano_lancamento AS "Ano de lançamento"
        
    from stg_netflix_show
    where tipo = 'Movie'

)

select *
from filmes