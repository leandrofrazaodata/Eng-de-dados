with stg_netflix_show as (

    select *
    from {{ ref('stg_netflix_show') }}

),

pais as (
    
    select 
        id_show,
        pais_origem,
        tipo 
        
    from stg_netflix_show
    where pais_origem is not null

),

qtd_show_pais as (

    select
        pais_origem AS "País de origem",
        tipo,
        count(distinct id_show) as "Quantidade de filmes por país"

    from pais
    group by 
        pais_origem,
        tipo

)

select *
from qtd_show_pais
order by 3 desc