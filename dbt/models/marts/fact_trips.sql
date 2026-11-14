-- Granularidad: una fila por viaje aceptado por la compuerta de calidad.
select
    cast(to_char(pickup_date, 'YYYYMMDD') as integer) as date_key,
    periodo,
    anio,
    pickup_hour,
    pu_location_id,
    do_location_id,
    payment_type,
    passenger_count,
    trip_distance,
    duracion_minutos,
    velocidad_mph,
    fare_amount,
    tip_amount,
    propina_pct,
    total_amount,
    cbd_congestion_fee,
    quality_flags
from {{ ref('stg_viajes') }}
