-- H3 footprint of all unit_of_property parcels that contain a horse training track (topo50)
CREATE TEMPORARY VIEW horse_training_properties_ AS
SELECT DISTINCT uop_all_h3.h3_index,
    'LINZ' AS source_data,
    DATERANGE(CURRENT_DATE, CURRENT_DATE, '[]') AS source_date,
    '[50,100]'::int4range AS source_scale
FROM unit_of_property_h3 uop_all_h3
JOIN unit_of_property uop_all USING (ogc_fid)
WHERE :parent::h3index = uop_all_h3.h3_partition
AND uop_all.unit_of_property_id IN (
    SELECT DISTINCT uop.unit_of_property_id
    FROM topo50_racetracks
    JOIN topo50_racetracks_h3 ON topo50_racetracks.ogc_fid = topo50_racetracks_h3.ogc_fid
    JOIN unit_of_property_h3 uop_h3 ON topo50_racetracks_h3.h3_index && uop_h3.h3_index
    JOIN unit_of_property uop ON uop_h3.ogc_fid = uop.ogc_fid
    WHERE :parent::h3index = topo50_racetracks_h3.h3_partition
    AND   :parent::h3index = uop_h3.h3_partition
    AND topo50_racetracks.track_type = 'training'
    AND topo50_racetracks.track_use  = 'horse'
);
