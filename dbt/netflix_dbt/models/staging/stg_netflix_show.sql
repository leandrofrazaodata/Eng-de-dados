with source_netflix_shows as (

    select * 

    from {{ source('netflix', 'netflix_shows') }}

),

renamed as (

    select 
        --ids
        show_id AS id_show,

        --num
        release_year AS ano_lancamento,

        --dt
        date_added AS data_adicao,

        --str
        type AS tipo,
        title AS titulo,
        rating AS classificacao,
        country AS pais_origem,
        director AS diretor,
        duration AS duracao,
        listed_in AS listado_em,
        description AS descricao,
        cast_members AS elenco,

        --metadados
        _airbyte_extracted_at AS data_extracao

    from source_netflix_shows

)

select *
from renamed