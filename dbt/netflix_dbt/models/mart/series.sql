with stg_netflix_show as (

    select *
    from {{ ref('stg_netflix_show') }}

),

series as (
    
    select 
        id_show AS "ID Série",
        titulo AS "Título da Série",
        classificacao AS "Classificação",
        ano_lancamento AS "Ano de lançamento"
        
    from stg_netflix_show
    where tipo = 'TV Show'

)

select *
from series