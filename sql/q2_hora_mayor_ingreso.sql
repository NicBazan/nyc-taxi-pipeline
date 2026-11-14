-- pregunta: La hora del dia con mayor ingreso promedio por viaje.
-- interpretacion: (completar despues de ver los numeros)
with por_hora as (
    select periodo, pickup_hour as hora,
        round(avg(total_amount)::numeric, 2) as ingreso_promedio,
        count(*) as viajes,
        rank() over (partition by periodo order by avg(total_amount) desc) as puesto
    from analytics.fact_trips
    group by periodo, pickup_hour
)
select periodo, hora, ingreso_promedio, viajes
from por_hora
where puesto = 1
order by periodo desc;
