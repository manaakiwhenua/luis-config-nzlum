-- Land in transition
-- As a subclass of ‘Production agriculture and plantations’, land categorised under this class must recently have been used for agriculture or plantations. In the case of greenfield development, where it is known that the land use is in transition to a built-environment category (e.g. due to a zoning change), classify the land under 3.9.0 ‘Vacant and transitioning land’, or a further subclass thereof.

-- Unused degraded land – unused land that is degraded through erosion or flood events that is not being rehabilitated. Can include contaminated land.

-- No defined use – land cleared of vegetation and where the current proposed land use is unknown.

-- Land undergoing rehabilitation – land in the process of rehabilitation for agricultural production (e.g. after significant flooding), and which is actively being recovered.

-- Abandoned land – land where a previous pattern of agriculture may be observed but that is not currently under production, but not due to physical land degradation.

CREATE TEMPORARY VIEW class_280 AS (
    SELECT h3_index,
    2 AS lu_code_primary,
    8 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN
            -- "Red Zone" etc., abandoned land (not in a settlement)
            transitional_land.h3_index IS NOT NULL
            AND urban_rural_2025_.IUR2025_V1_00 = '22' -- Rural other
        THEN ROW (
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[transitional_land.source_data]::TEXT[],
            transitional_land.source_date,
            transitional_land.source_scale
        )::nzlum_type
        WHEN (
            linz_dvr_.actual_property_use = '19' -- Rural vacant
            AND linz_dvr_.category ~ '^[ADFHOPS]'
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN linz_dvr_.category ~ '^.V' -- Vacant category
                THEN 1
                WHEN improvements_value < 100000
                THEN 1
                ELSE 2
            END, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale
        )::nzlum_type
        WHEN (
            linz_dvr_.actual_property_use = '29' -- Lifestyle vacant
            AND linz_dvr_.category ~ '^LV' -- i.e. without immediate subdivision potential
            AND urban_rural_2025_.IUR2025_V1_00 = '22' -- Rural other
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN improvements_description ~'\mORCHARD\M'
                THEN 5
                ELSE 1
            END, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale
        )::nzlum_type
        WHEN (
            linz_dvr_.actual_property_use = '00' ---- Multi-use at primary level: vacant or intermediate
            AND urban_rural_2025_.IUR2025_V1_00 = '22' -- Rural other
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN linz_dvr_.category ~ '^.V'
                THEN 1
                ELSE 2
            END, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale
        )::nzlum_type
        WHEN crop_transitional.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            6,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[crop_transitional.source_data]::TEXT[],
            crop_transitional.source_date,
            crop_transitional.source_scale
        )::nzlum_type
    END AS nzlum_type
    FROM (
        SELECT *
        FROM linz_dvr_
        WHERE actual_property_use IN (
            '00', 
            '19', -- Rural industry: vacant
            '29' -- Lifestyle: vacant
            -- '39', -- Transport: vacant
            -- '49', -- Community services: vacant
            -- '59', -- Recreational: vacant
            -- '69', -- Utility services: vacant
            -- '79', -- Industrial: vacant
            -- '89', -- Commercial: vacant
            -- '99' -- Residential: vacant
        )
        AND (
            "zone" ~ '^(0X|[1245][A-Z])'
            OR linz_dvr_.category ~ '^[ADFHLOPS]V?'
        )
    ) AS linz_dvr_
    INNER JOIN (
        SELECT *
        FROM lcdb_
        WHERE lcdb_.class_2018 IN (
            '10',
            '12',
            '16',
            '30',
            '33',
            '40',
            '41',
            '44',
            '51',
            '56',
            '64',
            '71'
        )
    ) AS lcdb_ USING (h3_index)
    FULL OUTER JOIN (
        SELECT
            urban_rural_2025_h3.h3_index,
            urban_rural_2025.IUR2025_V1_00
        FROM urban_rural_2025_h3
        JOIN urban_rural_2025 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS urban_rural_2025_ USING (h3_index)
    FULL OUTER JOIN transitional_land USING (h3_index)
    FULL OUTER JOIN (
        SELECT *
        FROM crop_maps
        WHERE (
            source_data = 'GDC'
            AND crop = 'To Be Planted'
        )
    ) AS crop_transitional USING (h3_index)
)

-- Lower confidence in urban areas, raise confidence in rural areas
-- DVR actual use codes:
    -- 00 Multi-use at primary level: vacant or intermediate
    -- 19 Rural industry: vacant
    -- 29 Lifestyle: vacant
    -- 39 Transport: vacant
    -- 49 Community services: vacant
    -- 59 Recreational: vacant
    -- 69 Utility services: vacant
    -- 79 Industrial: vacant
    -- 89 Commercial: vacant
    -- 99 Residential: vacant
-- DVR zones:
    -- 0X land in more than one zone or designation
    -- 1[A-Z] rural
    -- 2[A-Z] lifestyle
    -- 4[A-Z] community uses
    -- 5[A-Z] recreational
    -- Other zones are more non-agricultural
-- DVR category codes:
    -- [ADFHPS]
    -- NB : L --> Lifestyle land, generally in a rural area, where the predominant use is for a residence and, if vacant, there **is a right to build a dwelling.** (therefore this shuold be class 3)
    -- NB : FP - protected forests of any type (TODO so exclude from consideration)
    -- NB: FV (vacant)
    -- NB : LV (Vacant or substantially unimproved land **without immediate subdivision potential**)
-- DVR land with very low improvement value (rural zone etc)

-- TODO HAIL contaminated land
-- TODO Chch/Gisborne Red Zone in rural areas