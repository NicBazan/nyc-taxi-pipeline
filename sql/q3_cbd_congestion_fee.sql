-- pregunta: % de viajes con cbd_congestion_fee > 0 en el mes asignado y las 5 zonas de origen con mayor proporcion.
-- interpretacion: (completar despues de ver los numeros)
-- Solo zonas con al menos 1000 viajes en el mes, para que una zona con 3 viajes no gane con 100 %.
with del_mes as (
    select pu_location_id, cbd_congestion_fee > 0 as con_cargo
    from analytics.fact_trips
    where periodo = '{mes}'
),
total as (
    select round(avg(case when con_cargo then 1 else 0 end) * 100, 2) as pct_global from del_mes
)
select z.zone as zona_origen, z.borough as barrio,
    count(*) as viajes,
    round(avg(case when d.con_cargo then 1 else 0 end) * 100, 2) as pct_con_cargo,
    t.pct_global as pct_con_cargo_todo_el_mes
from del_mes d
join analytics.dim_zone z on z.location_id = d.pu_location_id
cross join total t
group by z.zone, z.borough, t.pct_global
having count(*) >= 1000
order by pct_con_cargo desc
limit 5;
