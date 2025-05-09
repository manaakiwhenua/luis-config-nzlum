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
            rural_other.h3_index IS NOT NULL
            AND transitional_land.h3_index IS NOT NULL
        THEN ROW (
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[transitional_land.source_data]::TEXT[],
            transitional_land.source_date,
            transitional_land.source_scale,
            NULL
        )::nzlum_type
        WHEN (
            lcdb_.h3_index IS NOT NULL
            AND linz_dvr_.actual_property_use = '19' -- Rural vacant
            AND linz_dvr_.category ~ '^[ADFHOPS]'
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN (
                    linz_dvr_.category LIKE '_V%' -- Vacant category
                    OR improvements_value < 100000
                )
                THEN 1
                ELSE 2
            END, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale,
            NULL
        )::nzlum_type
        WHEN (
            lcdb_.h3_index IS NOT NULL
            AND rural_other.h3_index IS NOT NULL
            AND linz_dvr_.actual_property_use = '29' -- Lifestyle vacant
            AND linz_dvr_.category LIKE 'LV%' -- i.e. without immediate subdivision potential
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN improvements_description ~ '\mORCHARDS?\M'
                THEN 5
                ELSE 1
            END, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale,
            NULL
        )::nzlum_type
        WHEN (
            lcdb_.h3_index IS NOT NULL
            AND rural_other.h3_index IS NOT NULL
            AND linz_dvr_.actual_property_use = '00' ---- Multi-use at primary level: vacant or intermediate
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN linz_dvr_.category LIKE '_V%'
                THEN 1
                ELSE 2
            END, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale,
            NULL
        )::nzlum_type
        WHEN crop_transitional.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            6,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[crop_transitional.source_data]::TEXT[],
            crop_transitional.source_date,
            crop_transitional.source_scale,
            NULL
        )::nzlum_type
    END AS nzlum_type
    FROM (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale,
            actual_property_use,
            category,
            improvements_value,
            improvements_description
        FROM linz_dvr_
        WHERE
            :parent::h3index = h3_partition
            AND actual_property_use IN (
                '00', 
                '19', -- Rural industry: vacant
                '29' -- Lifestyle: vacant
            )
            AND (
                "zone" ~ '^(0X|[1245][A-Z])'
                OR category ~ '^[ADFHLOPS]'
            )
    ) AS linz_dvr_
    LEFT JOIN (
        SELECT h3_index
        FROM lcdb_
        WHERE 
            :parent::h3index = h3_partition
            AND lcdb_.Class_2018 IN (
                10,
                12,
                16,
                30,
                33,
                40,
                41,
                44,
                51,
                56,
                64,
                71
            )
    ) AS lcdb_ USING (h3_index)
    FULL OUTER JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale
        FROM transitional_land
        WHERE :parent::h3index = h3_partition
    ) AS transitional_land USING (h3_index)
    FULL OUTER JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale
        FROM crop_maps
        WHERE 
            :parent::h3index = h3_partition
            AND (
                source_data = 'GDC'
                AND crop = 'To Be Planted'
            )
    ) AS crop_transitional USING (h3_index)
    LEFT JOIN (
        SELECT
            h3_index,
            IUR2025_V1_00
        FROM urban_rural_2025_h3
        JOIN urban_rural_2025 USING (ogc_fid)
        WHERE
            :parent::h3index = h3_partition
            AND IUR2025_V1_00 = '22' -- Rural other
    ) AS rural_other USING (h3_index)
)

-- Lower confidence in urban areas, raise confidence in rural areas
-- DVR actual use codes:
    -- 00 Multi-use at primary level: vacant or intermediate
    -- 19 Rural industry: vacant
    -- 29 Lifestyle: vacant
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
    -- NB : FP - protected forests of any type (TODO so exclude from consideration?)
    -- NB: FV (vacant)
    -- NB : LV (Vacant or substantially unimproved land **without immediate subdivision potential**)
-- DVR land with very low improvement value (rural zone etc)
-- Chch/Gisborne Red Zone in rural areas

-- TODO HAIL contaminated land?