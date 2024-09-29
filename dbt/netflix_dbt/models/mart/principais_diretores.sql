with stg_netflix_show as (

    select *
    from {{ ref('stg_netflix_show') }}

),

diretor as (
    
    select 
        id_show AS "ID Show",
        diretor AS Diretor
        
    from stg_netflix_show
    where diretor is not null

),

qtd_show_diretor as (

    select
        diretor AS "Nome do Diretor",
        count(distinct id_show) as "Quantidade de filmes por diretor"

    from diretor
    group by diretor

)

select *
from qtd_show_diretor
order by 2 desc