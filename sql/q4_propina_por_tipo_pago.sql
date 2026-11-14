-- pregunta: Propina promedio como % de la tarifa, por tipo de pago.
-- interpretacion: (completar despues de ver los numeros)
-- Se excluyen las filas con tarifa <= 0 (incluye la advertencia DQ09): con esa tarifa el % no tiene sentido.
select periodo,
    payment_type,
    case payment_type
        when 0 then 'Flex Fare / sin dato'
        when 1 then 'Tarjeta de credito'
        when 2 then 'Efectivo'
        when 3 then 'Sin cargo'
        when 4 then 'Disputa'
        else 'Otro'
    end as tipo_pago,
    count(*) as viajes,
    round(avg(propina_pct)::numeric, 2) as propina_promedio_pct
from analytics.fact_trips
where fare_amount > 0
group by periodo, payment_type
order by periodo desc, payment_type;
