CREATE TEMPORARY VIEW intensive_livestock_consents AS (
    WITH wrc_discharge_intensive AS (
        SELECT DISTINCT ON (uop_h3.h3_index)
            uop_h3.h3_index,
            c.source_date,
            c.source_data,
            c.source_scale,
            CASE
                WHEN c.PRIMARY_INDUSTRY_PURPOSE = 'Agricultural farming - pigs'
                THEN ARRAY['pigs']::TEXT[]
                WHEN c.PRIMARY_INDUSTRY_PURPOSE = 'Agricultural farming - poultry (broiler)'
                THEN ARRAY['chickens']::TEXT[]
                ELSE ARRAY[]::TEXT[]
            END AS commod
        FROM wrc_discharge_permits c
        JOIN wrc_discharge_permits_h3 c_h3 ON c.ogc_fid = c_h3.ogc_fid
        JOIN unit_of_property_h3 uop_inner_h3 ON c_h3.h3_index && uop_inner_h3.h3_index
        JOIN unit_of_property uop_inner ON uop_inner_h3.ogc_fid = uop_inner.ogc_fid
            AND ST_Within(c.geom, uop_inner.geom)
        JOIN unit_of_property uop_all ON uop_inner.unit_of_property_id = uop_all.unit_of_property_id
        JOIN unit_of_property_h3 uop_h3 ON uop_all.ogc_fid = uop_h3.ogc_fid
        WHERE :parent::h3index = c_h3.h3_partition
        AND   :parent::h3index = uop_inner_h3.h3_partition
        AND   :parent::h3index = uop_h3.h3_partition
        AND c.STATUS IN ('Current', 'Expired - S.124 Protection')
        AND c.PRIMARY_INDUSTRY_PURPOSE IN (
            'Agricultural farming - pigs',
            'Agricultural farming - poultry (broiler)'
        )
        ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
    ),
    trc_intensive_livestock AS (
        SELECT DISTINCT ON (uop_h3.h3_index)
            uop_h3.h3_index,
            c.source_date,
            c.source_data,
            c.source_scale,
            CASE
                WHEN c.authorisation_tsvector @@ to_tsquery('english', 'piggery')
                     THEN ARRAY['pigs']::TEXT[]
                WHEN c.authorisation_tsvector @@ to_tsquery('english', 'poultry')
                     THEN ARRAY['chickens']::TEXT[]
            END AS commod
        FROM trc_discharge_permits c
        JOIN trc_discharge_permits_h3 c_h3 ON c.ogc_fid = c_h3.ogc_fid
        JOIN unit_of_property_h3 uop_inner_h3 ON c_h3.h3_index && uop_inner_h3.h3_index
        JOIN unit_of_property uop_inner ON uop_inner_h3.ogc_fid = uop_inner.ogc_fid
            AND ST_Within(c.geom, uop_inner.geom)
        JOIN unit_of_property uop_all ON uop_inner.unit_of_property_id = uop_all.unit_of_property_id
        JOIN unit_of_property_h3 uop_h3 ON uop_all.ogc_fid = uop_h3.ogc_fid
        WHERE :parent::h3index = c_h3.h3_partition
        AND   :parent::h3index = uop_inner_h3.h3_partition
        AND   :parent::h3index = uop_h3.h3_partition
        AND c.status = 'Current'
        AND c.authorisation_tsvector @@ to_tsquery('english', 'piggery | poultry')
        ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
    )
    SELECT * FROM wrc_discharge_intensive
    UNION ALL
    SELECT * FROM trc_intensive_livestock
);
