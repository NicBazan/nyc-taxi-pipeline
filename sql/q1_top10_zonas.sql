-- pregunta: Las 10 zonas de origen con mas viajes y su variacion contra el mismo mes del anio anterior.
-- interpretacion: (completar despues de ver los numeros)
with viajes as (
    select pu_location_id,
        count(*) filter (where periodo = '{mes}') as viajes_mes,
        count(*) filter (where periodo = '{mes_anterior}') as viajes_anterior
    from analytics.fact_trips
    group by pu_location_id
)
select z.zone as zona_origen, z.borough as barrio,
    v.viajes_mes, v.viajes_anterior,
    round((v.viajes_mes - v.viajes_anterior) * 100.0 / nullif(v.viajes_anterior, 0), 1) as variacion_pct
from viajes v
join analytics.dim_zone z on z.location_id = v.pu_location_id
order by v.viajes_mes desc
limit 10;
