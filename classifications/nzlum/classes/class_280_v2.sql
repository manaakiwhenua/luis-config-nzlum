-- As a subclass of Production agriculture and plantations, land must recently have been used for agriculture or plantations.
-- In the case of greenfield development where land use is transitioning to a built-environment category, classify under 3.9.0 Vacant and transitioning land (or a subclass) rather than 2.8.0.

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
            10, -- Sand or Gravel
            12, -- Landslide
            16, -- Gravel or Rock
            30, -- Short-rotation Cropland
            33, -- Orchards, Vineyards or Other Perennial Crops
            40, -- High Producing Exotic Grassland
            41, -- Low Producing Grassland
            44, -- Depleted Grassland
            51, -- Gorse and/or Broom
            56, -- Mixed Exotic Shrubland
            64, -- Forest - Harvested
            71  -- Exotic Forest
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
    FROM urban_rural_current_h3
    JOIN urban_rural_current USING (ogc_fid)
    WHERE
        :parent::h3index = h3_partition
        AND IUR2026_V1_00 = '22'
),

-- UNION ALL of all potential matches
unioned_matches AS (
    -- Match 1: Transitional land in rural_other
    SELECT
        roi.h3_index,
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
    FROM roi
    JOIN filtered_transitional t ON roi.h3_index && t.h3_index
    JOIN filtered_rural_other r ON roi.h3_index && r.h3_index

    UNION ALL

    -- Match 2: LINZ vacant rural
    SELECT
        roi.h3_index,
        2, 8, 0,
        ROW(
            ARRAY[]::TEXT[],
            CASE
                WHEN l.zone !~ '^[01]' -- Not zoned for rural (or not mixed zone)
                    THEN 8
                WHEN filtered_lcdb.Class_2018 = 71 -- Exotic Forest
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
    FROM roi
    JOIN filtered_linz l ON roi.h3_index && l.h3_index
    JOIN filtered_lcdb ON roi.h3_index && filtered_lcdb.h3_index
    WHERE
        l.actual_property_use = '19'
        AND l.category ~ '^[ADFHOPS]'

    UNION ALL

    -- Match 3: Lifestyle vacant with orchard hint
    -- Orchard improvements on a vacant property suggest past agricultural use,
    -- not necessarily active production. Active orchards → class 2.3.x.
    SELECT
        roi.h3_index,
        2, 8, 0,
        ROW(
            ARRAY[]::TEXT[],
            CASE
                WHEN l.improvements_description ~ '\mORCHARDS?\M'
                    THEN CASE
                        WHEN l.zone ~ '^1' THEN 1 -- Orchard improvements in rural zone
                        WHEN l.zone ~ '^0' THEN 3 -- Orchard improvements, multiple zones
                        ELSE 4                    -- Orchard improvements, other zone
                    END
                WHEN l.zone ~ '^1' THEN 3         -- Rural zone, no orchard hint
                WHEN l.zone ~ '^0' THEN 5         -- Multiple zones, no orchard hint
                ELSE 7
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[l.source_data]::TEXT[],
            l.source_date,
            l.source_scale,
            NULL
        )::nzlum_type,
        3
    FROM roi
    JOIN filtered_linz l ON roi.h3_index && l.h3_index
    JOIN filtered_lcdb ON roi.h3_index && filtered_lcdb.h3_index
    JOIN filtered_rural_other ON roi.h3_index && filtered_rural_other.h3_index
    WHERE
        l.actual_property_use = '29'
        AND l.category LIKE 'LV%'

    UNION ALL

    -- Match 4: LINZ '00' multi-use
    SELECT
        roi.h3_index,
        2, 8, 0,
        ROW(
            ARRAY[]::TEXT[],
            CASE
                WHEN l.category LIKE '_V%'
                    THEN 3
                WHEN filtered_lcdb.Class_2018 = 71 -- Exotic Forest
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
    FROM roi
    JOIN filtered_linz l ON roi.h3_index && l.h3_index
    JOIN filtered_lcdb ON roi.h3_index && filtered_lcdb.h3_index
    JOIN filtered_rural_other ON roi.h3_index && filtered_rural_other.h3_index
    WHERE l.actual_property_use = '00'

    UNION ALL

    -- Match 5: Crop transitional
    SELECT
        roi.h3_index,
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
    FROM roi
    JOIN filtered_crop_transitional c ON roi.h3_index && c.h3_index
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