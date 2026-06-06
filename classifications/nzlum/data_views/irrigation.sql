CREATE TEMPORARY VIEW irrigation_ AS (
    WITH irrigation_mapping AS (
        SELECT DISTINCT ON (h3_index)
            h3_index,
            h3_partition,
            ts_notes,
            "status",
            irrigation_type,
            confidence,
            daterange(
                TO_DATE(year_irr::TEXT, 'YYYY'),
                TO_DATE((1 + year_irr)::TEXT, 'YYYY'),
                '[]'
            ) AS source_date,
            'MfE' AS source_data,
            '(0,1]'::int4range AS source_scale, -- 0.075 m to 0.3 m 
            CASE
                WHEN irrigation_type IN (
                    'Wild flooding',
                    'Borderdyke', 'Border dyke'
                )
                THEN 'irrigation surface'
                WHEN irrigation_type IN (
                    'Solid-set',
                    'Side Roll',
                    'Rotorainer',
                    'Pivot',
                    'Linear boom',
                    'Lateral',
                    'K-line/Long lateral',
                    'Gun'
                )
                THEN 'irrigation spray'
                WHEN irrigation_type IN (
                    'Drip/micro', 'Drip/Micro', 'Drip/ Micro'
                )
                THEN 'irrigation drip'
                ELSE 'irrigation'
            END AS manage,
            irrigation_2020.irrigation_age
    FROM irrigation_2020_h3
    INNER JOIN irrigation_2020 USING (ogc_fid)
    WHERE :parent::h3index = h3_partition
    -- NB reasons for potential further exclusions in notes (ts_notes) e.g. notes = 'Golf course'
    ORDER BY
        h3_index,
        CASE "status"
            WHEN 'Current' THEN 1
            WHEN 'Superseded' THEN 2
            ELSE 3
        END,
        CASE confidence
            WHEN 'High' THEN 1
            WHEN 'Medium' THEN 2
            WHEN 'Low' THEN 3
            ELSE 4
        END,
        year_irr DESC NULLS LAST,
        yearmapped DESC NULLS LAST,
        area_ha DESC NULLS LAST
    ),
    irrigation_consents AS (
        SELECT DISTINCT ON (h3_index)
            h3_index,
            h3_partition,
            NULL::tsvector AS ts_notes,
            'Current' AS "status",
            'Low' AS confidence, -- Other source is more spatially explicit, but his handles other cases and is evergreen
            NULL AS irrigation_type,
            source_date,
            source_data,
            source_scale,
            'irrigation'::TEXT AS manage,
            NULL::int AS irrigation_age
        FROM ecan_consented_activities_areas_active
        JOIN ecan_consented_activities_areas_active_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
            AND current = TRUE
            AND FeatureType = 'Irrigation Area'
        ORDER BY
            h3_index,
            fmDate DESC NULLS LAST, -- Most recently issued
            toDate DESC NULLS LAST, -- Latest expiry
            area_ha, -- Prefer larger
            link -- Prefer with more information
    ),
    ecan_irrigation_consents_point AS (
        SELECT DISTINCT ON (uop_h3.h3_index)
            uop_h3.h3_index,
            uop_h3.h3_partition,
            NULL::tsvector AS ts_notes,
            'Current' AS "status",
            'Low' AS confidence,
            NULL AS irrigation_type,
            c.source_date,
            c.source_data,
            c.source_scale,
            'irrigation'::TEXT AS manage,
            NULL::int AS irrigation_age
        FROM ecan_consented_activities_points_active c
        JOIN ecan_consented_activities_points_active_h3 c_h3 ON c.ogc_fid = c_h3.ogc_fid
        JOIN unit_of_property_h3 uop_inner_h3 ON c_h3.h3_index && uop_inner_h3.h3_index
        JOIN unit_of_property uop_inner ON uop_inner_h3.ogc_fid = uop_inner.ogc_fid
            AND ST_Within(c.geom, uop_inner.geom)
        JOIN unit_of_property uop_all ON uop_inner.unit_of_property_id = uop_all.unit_of_property_id
        JOIN unit_of_property_h3 uop_h3 ON uop_all.ogc_fid = uop_h3.ogc_fid
        WHERE :parent::h3index = c_h3.h3_partition
        AND   :parent::h3index = uop_inner_h3.h3_partition
        AND   :parent::h3index = uop_h3.h3_partition
        AND c.current = TRUE
        AND c.FeatureType = 'Irrigation Area'
        ORDER BY uop_h3.h3_index, start_date DESC NULLS LAST, end_date DESC NULLS LAST
    )
    SELECT *
    FROM irrigation_mapping

    -- union with anti-joins: prefer irrigation_mapping > area consents > point consents
    UNION ALL

    SELECT *
    FROM irrigation_consents
    WHERE NOT EXISTS (
        SELECT 1
        FROM irrigation_mapping
        WHERE irrigation_mapping.h3_index && irrigation_consents.h3_index
    )

    UNION ALL

    SELECT *
    FROM ecan_irrigation_consents_point
    WHERE NOT EXISTS (
        SELECT 1
        FROM irrigation_mapping
        WHERE irrigation_mapping.h3_index && ecan_irrigation_consents_point.h3_index
    )
    AND NOT EXISTS (
        SELECT 1
        FROM irrigation_consents
        WHERE irrigation_consents.h3_index && ecan_irrigation_consents_point.h3_index
    )
);