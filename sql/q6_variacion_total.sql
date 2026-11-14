-- pregunta: Variacion de viajes totales y de tarifa media entre los dos meses.
-- interpretacion: (completar despues de ver los numeros; hipotesis en no mas de 5 lineas)
with por_mes as (
    select periodo, count(*) as viajes,
        avg(fare_amount) filter (where fare_amount > 0) as tarifa_media
    from analytics.fact_trips
    where periodo in ('{mes}', '{mes_anterior}')
    group by periodo
)
select a.viajes as viajes_mes, b.viajes as viajes_anterior,
    round((a.viajes - b.viajes) * 100.0 / b.viajes, 2) as variacion_viajes_pct,
    round(a.tarifa_media::numeric, 2) as tarifa_media_mes,
    round(b.tarifa_media::numeric, 2) as tarifa_media_anterior,
    round(((a.tarifa_media - b.tarifa_media) * 100 / b.tarifa_media)::numeric, 2) as variacion_tarifa_pct
from por_mes a, por_mes b
where a.periodo = '{mes}' and b.periodo = '{mes_anterior}';
