CREATE TEMPORARY VIEW linz_crosl_ AS (
    SELECT DISTINCT ON (h3_index) h3_index,
    managed_by, -- e.g. "Housing New Zealand"
    statutory_actions,
    area_ha,
    'LINZ CRoSL' AS source_data,
    daterange(
        LEAST(
            COALESCE(
                TO_DATE(date_updated, 'YYYYMMDD'), -- Feature update date
                '2024-08-22'::DATE  -- CRoSL metadata reference date - revision
            )
        ),
        GREATEST(
            COALESCE(
                TO_DATE(date_updated, 'YYYYMMDD'),
                '2024-08-22'::DATE
            )
        ),
        '[]'::TEXT
    ) AS source_date,
    CASE
        WHEN urban_rural_2025_.IUR2025_V1_00 IN (
            '11', -- major urban
            '12', -- large urban
            '13', -- medium urban
            '14' -- small urban
        ) -- 0.1 - 1 m in urban areas
        THEN '(0,1]'::int4range
        WHEN urban_rural_2025_.IUR2025_V1_00 IN (
            '21', -- rural settlement
            '22' -- rural other
        ) -- 1 - 100 m in rural areas
        THEN '[1,100]'::int4range
        -- inland water, inlet, oceanic
        ELSE '[1,100]'::int4range
    END AS source_scale
    FROM (
        SELECT *
        FROM crosl
        JOIN crosl_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS crosl_
    LEFT JOIN (
        SELECT
            urban_rural_2025_h3.h3_index,
            urban_rural_2025.IUR2025_V1_00
        FROM urban_rural_2025_h3
        JOIN urban_rural_2025 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS urban_rural_2025_ USING (h3_index)
    ORDER BY
        h3_index,
        TO_DATE(date_updated, 'YYYYMMDD'),
        managed_by,
        area_ha
);