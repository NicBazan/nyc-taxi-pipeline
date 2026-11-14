-- Granularidad: una fila por zona de origen, fecha y hora de subida.
select
    f.periodo,
    f.pu_location_id,
    f.date_key,
    f.pickup_hour,
    count(*) as viajes,
    round(sum(f.total_amount)::numeric, 2) as ingreso_total,
    round(avg(f.duracion_minutos)::numeric, 2) as duracion_media_min
from {{ ref('fact_trips') }} f
group by 1, 2, 3, 4
