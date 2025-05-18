CREATE TEMPORARY VIEW class_280 AS
WITH
-- CTEs to filter each source
filtered_linz AS (
    SELECT
        h3_index,
        source_data,
        source_date,
        source_scale,
        actual_property_use,
        category,
        "zone",
        improvements_value,
        improvements_description
    FROM linz_dvr_
    WHERE
        :parent::h3index = h3_partition
        AND actual_property_use IN ('00', '19', '29')
        AND (
            "zone" ~ '^(0X|[1245][A-Z])'
            OR category ~ '^[ADFHLOPS]'
        )
),
filtered_lcdb AS (
    SELECT h3_index, Class_2018
    FROM lcdb_
    WHERE
        :parent::h3index = h3_partition
        AND Class_2018 IN (
            10, 12, 16, 30, 33, 40,
            41, 44, 51, 56, 64, 71
        )
),
filtered_transitional AS (
    SELECT h3_index, source_data, source_date, source_scale
    FROM transitional_land
    WHERE :parent::h3index = h3_partition
),
filtered_crop_transitional AS (
    SELECT h3_index, source_data, source_date, source_scale
    FROM crop_maps
    WHERE
        :parent::h3index = h3_partition
        AND (
            source_data = 'GDC'
            AND 'To Be Planted' = ANY(crop)
        )
),
filtered_rural_other AS (
    SELECT h3_index
    FROM urban_rural_2025_h3
    JOIN urban_rural_2025 USING (ogc_fid)
    WHERE
        :parent::h3index = h3_partition
        AND IUR2025_V1_00 = '22'
),

-- UNION ALL of all potential matches
unioned_matches AS (
    -- Match 1: Transitional land in rural_other
    SELECT
        t.h3_index,
        2 AS lu_code_primary,
        8 AS lu_code_secondary,
        0 AS lu_code_tertiary,
        ROW(
            ARRAY[]::TEXT[],
            1,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[t.source_data]::TEXT[],
            t.source_date,
            t.source_scale,
            NULL
        )::nzlum_type AS nzlum_type,
        1 AS priority
    FROM filtered_transitional t
    INNER JOIN filtered_rural_other r USING (h3_index)

    UNION ALL

    -- Match 2: LINZ vacant rural
    SELECT
        l.h3_index,
        2, 8, 0,
        ROW(
            ARRAY[]::TEXT[],
            CASE
                WHEN l.zone !~ '^[01]' -- Not zoned for rural (or not mixed zone)
                    THEN 8
                WHEN filtered_lcdb.Class_2018 = 71 -- Exotic forest
                    THEN 9
                WHEN (
                    l.category LIKE '_V%' -- Vacant
                    OR l.improvements_value < 100000 -- No considerable improvements built
                )
                    THEN 1
                ELSE 2
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[l.source_data]::TEXT[],
            l.source_date,
            l.source_scale,
            NULL
        )::nzlum_type,
        2
    FROM filtered_linz l
    INNER JOIN filtered_lcdb USING (h3_index)
    WHERE
        l.actual_property_use = '19'
        AND l.category ~ '^[ADFHOPS]'

    UNION ALL

    -- Match 3: Lifestyle vacant with orchard hint
    SELECT
        l.h3_index,
        2, 8, 0,
        ROW(
            ARRAY[]::TEXT[],
            CASE
                WHEN l.zone ~ '^1' -- Zoned for rural use
                    THEN 1
                WHEN l.improvements_description ~ '\mORCHARDS?\M'
                    THEN 4
                WHEN l.zone ~ '^0' -- More than one zone
                    THEN 5
                ELSE 6
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[l.source_data]::TEXT[],
            l.source_date,
            l.source_scale,
            NULL
        )::nzlum_type,
        3
    FROM filtered_linz l
    JOIN filtered_lcdb USING (h3_index)
    JOIN filtered_rural_other USING (h3_index)
    WHERE
        l.actual_property_use = '29'
        AND l.category LIKE 'LV%'

    UNION ALL

    -- Match 4: LINZ '00' multi-use
    SELECT
        l.h3_index,
        2, 8, 0,
        ROW(
            ARRAY[]::TEXT[],
            CASE
                WHEN l.category LIKE '_V%'
                    THEN 3
                WHEN filtered_lcdb.Class_2018 = 71 -- Exotic forest
                    THEN 9
                ELSE 4
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[l.source_data]::TEXT[],
            l.source_date,
            l.source_scale,
            NULL
        )::nzlum_type,
        4
    FROM filtered_linz l
    JOIN filtered_lcdb USING (h3_index)
    JOIN filtered_rural_other USING (h3_index)
    WHERE l.actual_property_use = '00'

    UNION ALL

    -- Match 5: Crop transitional
    SELECT
        c.h3_index,
        2, 8, 0,
        ROW(
            ARRAY[]::TEXT[],
            6,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[c.source_data]::TEXT[],
            c.source_date,
            c.source_scale,
            NULL
        )::nzlum_type,
        5
    FROM filtered_crop_transitional c
),

-- Pick best match per h3_index
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
                PARTITION BY h3_index
                ORDER BY
                    (nzlum_type).confidence ASC,
                    priority ASC,
                    upper((nzlum_type).source_date) DESC NULLS LAST,
                    lower((nzlum_type).source_date) ASC NULLS LAST,
                    (nzlum_type).source_scale ASC NULLS LAST,
                    (nzlum_type).source_data ASC NULLS LAST
            ) AS rn
    FROM unioned_matches
)

-- Final result: one row per h3_index
SELECT
    h3_index,
    lu_code_primary,
    lu_code_secondary,
    lu_code_tertiary,
    nzlum_type
FROM ranked
WHERE rn = 1;