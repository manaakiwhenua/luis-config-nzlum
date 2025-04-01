CREATE TEMPORARY VIEW irrigation_ AS (
    SELECT
        DISTINCT ON (h3_index) *,
        daterange(
            TO_DATE(year_irr::TEXT, 'YYYY'),
            TO_DATE((1 + year_irr)::TEXT, 'YYYY'),
            '[]'
        ) AS source_date,
        'MfE' AS source_data,
        '(0,1)'::int4range AS source_scale, -- 0.075 m to 0.3 m 
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
    -- NB other reasons for potential further exclusions in notes (ts_notes) e.g. notes = 'Golf course'
);