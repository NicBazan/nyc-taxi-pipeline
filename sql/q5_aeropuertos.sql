-- pregunta: Cantidad de viajes y tarifa media de los que empiezan o terminan en JFK o LaGuardia.
-- interpretacion: (completar despues de ver los numeros)
-- Un viaje JFK -> LaGuardia se cuenta una sola vez porque el filtro es por viaje (OR), no por zona.
select f.periodo,
    count(*) as viajes,
    round(avg(f.fare_amount) filter (where f.fare_amount > 0)::numeric, 2) as tarifa_media
from analytics.fact_trips f
join analytics.dim_zone pu on pu.location_id = f.pu_location_id
join analytics.dim_zone dz on dz.location_id = f.do_location_id
where pu.zone in ('JFK Airport', 'LaGuardia Airport')
   or dz.zone in ('JFK Airport', 'LaGuardia Airport')
group by f.periodo
order by f.periodo desc;
