-- Granularidad: una fila por zona de la TLC (location_id 1 a 265).
select
    location_id,
    coalesce(borough, 'Unknown') as borough,
    coalesce(zone, 'Unknown') as zone,
    coalesce(service_zone, 'Unknown') as service_zone
from {{ source('curado', 'zonas') }}
