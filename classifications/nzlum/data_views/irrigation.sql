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
                    'Linear boom'
                    'Lateral'
                    'K-line/Long lateral',
                    'Gun'
                )
                THEN 'irrigation spray'
                WHEN irrigation_type IN (
                    'Drip/micro', 'Drip/Micro', 'Drip/ Micro'
                )
                THEN 'irrigation drip'
                ELSE 'irrigation'
            END AS manage
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
            'irrigation'::TEXT AS manage
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
    )
    SELECT *
    FROM irrigation_mapping

    -- union with an anti-join
    -- i.e. prefer irrigation_mapping, but supplement it with information from irrigation_consents
    UNION ALL

    SELECT *
    FROM irrigation_consents
    WHERE NOT EXISTS (
        SELECT 1
        FROM irrigation_mapping
        WHERE irrigation_mapping.h3_index = irrigation_consents.h3_index
    )
);