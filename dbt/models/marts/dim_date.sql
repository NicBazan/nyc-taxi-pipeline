-- Granularidad: una fila por dia calendario que aparece en los viajes.
select distinct
    cast(to_char(pickup_date, 'YYYYMMDD') as integer) as date_key,
    pickup_date as fecha,
    extract(year from pickup_date)::int as anio,
    extract(month from pickup_date)::int as mes,
    extract(day from pickup_date)::int as dia,
    extract(isodow from pickup_date)::int as dia_semana,
    extract(isodow from pickup_date) in (6, 7) as es_fin_de_semana
from {{ ref('stg_viajes') }}
