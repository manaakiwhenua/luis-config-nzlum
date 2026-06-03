-- Vacant and transitioning land

-- This land-use type includes areas that are currently unused or undergoing a transition from one land use to another, but in this case with a clear transition towards or within other concepts under the built environment.

--     Vacant land – includes derelict land and developed land that is idle.

--     Greenfield development – previously undeveloped or agricultural land zones for or undergoing new construction projects or urban expansion, typically involving the conversion of rural or natural areas into residential, commercial, industrial, or infrastructural uses.

--     Brownfield development – areas of active redevelopment of previously developed (often industrial) land that may be abandoned, contaminated, or economically under-utilised, with the aim of rehabilitating and repurposing these sites for new urban activities. May include residential areas undergoing infill development that increase housing density.

CREATE TEMPORARY VIEW class_390 AS (
    SELECT roi.h3_index,
    3 AS lu_code_primary,
    9 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN
            transitional_land.h3_index IS NOT NULL
            AND urban_rural_current_.h3_index IS NOT NULL
        THEN CASE
            WHEN linz_dvr_full_.h3_index IS NULL
            THEN ROW(
                ARRAY[]::TEXT[], -- lu_code_ancillary
                LEAST(12, 1 + CASE
                    WHEN lcdb_horticulture.h3_index IS NOT NULL THEN 4 -- LCDB shows active cropland/horticulture; contradicts vacant classification
                    ELSE 0
                END),
                ARRAY[]::TEXT[], -- commod
                ARRAY[]::TEXT[], -- manage
                ARRAY[transitional_land.source_data, lcdb_horticulture.source_data]::TEXT[],
                range_merge(datemultirange(VARIADIC ARRAY_REMOVE(ARRAY[
                    transitional_land.source_date,
                    lcdb_horticulture.source_date
                ], NULL))),
                range_merge(int4multirange(VARIADIC ARRAY_REMOVE(ARRAY[
                    transitional_land.source_scale,
                    lcdb_horticulture.source_scale
                ], NULL))),
                NULL
            )::nzlum_type
            WHEN linz_dvr_full_.actual_property_use = '29' -- Lifestyle: vacant (less certain — lifestyle blocks are often not truly transitioning)
            THEN ROW(
                ARRAY[]::TEXT[], -- lu_code_ancillary
                LEAST(12, 4 + CASE
                    WHEN lcdb_horticulture.h3_index IS NOT NULL THEN 4 -- LCDB shows active cropland/horticulture; contradicts vacant classification
                    ELSE 0
                END),
                ARRAY[]::TEXT[], -- commod
                ARRAY[]::TEXT[], -- manage
                ARRAY[transitional_land.source_data, linz_dvr_full_.source_data, lcdb_horticulture.source_data]::TEXT[],
                range_merge(datemultirange(VARIADIC ARRAY_REMOVE(ARRAY[
                    transitional_land.source_date,
                    linz_dvr_full_.source_date,
                    lcdb_horticulture.source_date
                ], NULL))),
                range_merge(int4multirange(VARIADIC ARRAY_REMOVE(ARRAY[
                    transitional_land.source_scale,
                    linz_dvr_full_.source_scale,
                    lcdb_horticulture.source_scale
                ], NULL))),
                NULL
            )::nzlum_type
            WHEN (
                linz_dvr_full_.actual_property_use ~ '[13-9]9$' -- Vacant (built-environment or rural-industry codes)
                OR linz_dvr_full_.actual_property_use IN (
                    '00', -- Vacant
                    '55' -- Passive outdoor
                )
                OR linz_dvr_full_.actual_property_use IS NULL
            )
            THEN ROW(
                ARRAY[]::TEXT[], -- lu_code_ancillary
                LEAST(12, 1 + CASE
                    WHEN lcdb_horticulture.h3_index IS NOT NULL THEN 4 -- LCDB shows active cropland/horticulture; contradicts vacant classification
                    ELSE 0
                END),
                ARRAY[]::TEXT[], -- commod
                ARRAY[]::TEXT[], -- manage
                ARRAY[transitional_land.source_data, linz_dvr_full_.source_data, lcdb_horticulture.source_data]::TEXT[],
                range_merge(datemultirange(VARIADIC ARRAY_REMOVE(ARRAY[
                    transitional_land.source_date,
                    linz_dvr_full_.source_date,
                    lcdb_horticulture.source_date
                ], NULL))),
                range_merge(int4multirange(VARIADIC ARRAY_REMOVE(ARRAY[
                    transitional_land.source_scale,
                    linz_dvr_full_.source_scale,
                    lcdb_horticulture.source_scale
                ], NULL))),
                NULL
            )::nzlum_type
            WHEN linz_dvr_full_.category ~ '\wV'
            THEN ROW(
                ARRAY[]::TEXT[], -- lu_code_ancillary
                LEAST(12, CASE
                    WHEN linz_dvr_full_.zone ~ '^[789]' -- Higher confidence when also zoned for industrial, commercial, residential
                    THEN 8
                    ELSE 10
                END + CASE
                    WHEN lcdb_horticulture.h3_index IS NOT NULL THEN 4 -- LCDB shows active cropland/horticulture; contradicts vacant classification
                    ELSE 0
                END),
                ARRAY[]::TEXT[], -- commod
                ARRAY[]::TEXT[], -- manage
                ARRAY[transitional_land.source_data, linz_dvr_full_.source_data, lcdb_horticulture.source_data]::TEXT[],
                range_merge(datemultirange(VARIADIC ARRAY_REMOVE(ARRAY[
                    transitional_land.source_date,
                    linz_dvr_full_.source_date,
                    lcdb_horticulture.source_date
                ], NULL))),
                range_merge(int4multirange(VARIADIC ARRAY_REMOVE(ARRAY[
                    transitional_land.source_scale,
                    linz_dvr_full_.source_scale,
                    lcdb_horticulture.source_scale
                ], NULL))),
                NULL
            )::nzlum_type
            END
        WHEN linz_dvr_vacant_built.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            clamp_confidence_or_null(CASE
                WHEN linz_dvr_vacant_built.actual_property_use IN (
                    '39', -- Transport: vacant
                    '49', -- Community services: vacant
                    '59', -- Recreational: vacant
                    '69', -- Utility services: vacant
                    '79', -- Industrial: vacant
                    '89', -- Commercial: vacant
                    '99' -- Residential: vacant
                )
                THEN
                    CASE
                        WHEN linz_dvr_vacant_built.zone ~ '^[24789].*' AND low_ratio_improvements
                        THEN 1
                        WHEN linz_dvr_vacant_built.zone ~ '^[24789].*'
                        THEN 2
                        ELSE 5
                    END
                WHEN linz_dvr_vacant_built.actual_property_use = '00' -- Multi-use at primary level: Vacant or intermediate
                THEN
                    CASE
                        WHEN linz_dvr_vacant_built.zone ~ '^[24789].*' AND low_ratio_improvements
                        THEN 3
                        WHEN linz_dvr_vacant_built.zone ~ '^[24789].*'
                        THEN 4
                        ELSE NULL
                    END
                WHEN linz_dvr_vacant_built.actual_property_use = '29' -- Lifestyle: vacant (less certain — lifestyle blocks are often not truly transitioning)
                THEN
                    CASE
                        WHEN linz_dvr_vacant_built.zone ~ '^[24789].*' AND low_ratio_improvements
                        THEN 5
                        WHEN linz_dvr_vacant_built.zone ~ '^[24789].*'
                        THEN 6
                        ELSE NULL
                    END
                WHEN (
                    linz_dvr_vacant_built.actual_property_use = '19' -- Rural vacant
                    AND (
                        linz_dvr_vacant_built.category ~ '^L[BIV]' -- Bare, improved or vacant lifestyle category
                        OR linz_dvr_vacant_built.category ~ '^[CIR]V' --vacant commercial/industrial/residential category
                    )
                )
                THEN
                    CASE
                        WHEN linz_dvr_vacant_built.zone ~ '^[24789].*' AND low_ratio_improvements
                        THEN 1
                        WHEN linz_dvr_vacant_built.zone ~ '^[24789].*'
                        THEN 2
                        ELSE 5
                    END
            END
            + CASE -- Penalise by valuation recency: older vacancy records are less reliable
                WHEN linz_dvr_vacant_built.current_effective_valuation_date IS NULL
                THEN 2
                WHEN linz_dvr_vacant_built.current_effective_valuation_date > CURRENT_DATE - INTERVAL '1 year'
                THEN 0
                WHEN linz_dvr_vacant_built.current_effective_valuation_date > CURRENT_DATE - INTERVAL '3 years'
                THEN 1
                WHEN linz_dvr_vacant_built.current_effective_valuation_date > CURRENT_DATE - INTERVAL '5 years'
                THEN 2
                ELSE 3
            END
            + CASE -- LCDB horticulture signal contradicts a vacant/transitioning classification
                WHEN lcdb_horticulture.h3_index IS NOT NULL THEN 4 -- Short-rotation cropland or orchards/vineyards/perennial crops
                ELSE 0
            END), -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_vacant_built.source_data, lcdb_horticulture.source_data]::TEXT[],
            range_merge(datemultirange(VARIADIC ARRAY_REMOVE(ARRAY[
                linz_dvr_vacant_built.source_date,
                lcdb_horticulture.source_date
            ], NULL))),
            range_merge(int4multirange(VARIADIC ARRAY_REMOVE(ARRAY[
                linz_dvr_vacant_built.source_scale,
                lcdb_horticulture.source_scale
            ], NULL))),
            NULL
        )::nzlum_type
    END AS nzlum_type
    FROM roi
    LEFT JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale,
            current_effective_valuation_date,
            actual_property_use,
            category,
            "zone",
            (improvements_value_ratio < 0.1) AS low_ratio_improvements
        FROM linz_dvr_
        WHERE
            actual_property_use IN (
                '00', -- Multi-use at primary level: Vacant or intermediate
                '19',
                '29', -- Lifestyle: vacant
                '39', -- Transport: vacant
                '49', -- Community services: vacant
                '59', -- Recreational: vacant
                '69', -- Utility services: vacant
                '79', -- Industrial: vacant
                '89', -- Commercial: vacant
                '99' -- Residential: vacant
            )
            OR "zone" ~ '^[24789].*' -- Lifestyle, community use, recreational, industrial, commercial or residential zones
            OR "zone" = '0X' -- Land in more than one zone or designation
    ) AS linz_dvr_vacant_built ON roi.h3_index && linz_dvr_vacant_built.h3_index
    LEFT JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale,
            actual_property_use,
            category,
            "zone"
        FROM linz_dvr_
    ) AS linz_dvr_full_ ON roi.h3_index && linz_dvr_full_.h3_index
    LEFT JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale
        FROM transitional_land
    ) AS transitional_land ON roi.h3_index && transitional_land.h3_index
    LEFT JOIN (
        SELECT
            h3_index,
            IUR2026_V1_00
        FROM urban_rural_current_h3
        JOIN urban_rural_current USING (ogc_fid)
        WHERE
            :parent::h3index = h3_partition
            AND urban_rural_current.IUR2026_V1_00 <> '22' -- (non) "Rural other"
    ) AS urban_rural_current_ ON roi.h3_index && urban_rural_current_.h3_index
    LEFT JOIN (
        SELECT h3_index, source_data, source_date, source_scale
        FROM lcdb_
        WHERE
            :parent::h3index = h3_partition
            AND Class_2023 IN (
                30, -- Short-rotation Cropland
                33  -- Orchards, Vineyards or Other Perennial Crops
            )
    ) AS lcdb_horticulture ON roi.h3_index && lcdb_horticulture.h3_index
    LEFT JOIN water_features ON roi.h3_index && water_features.h3_index
    LEFT JOIN pan_nz_draft_h3 ON roi.h3_index && pan_nz_draft_h3.h3_index
    WHERE (transitional_land.h3_index IS NOT NULL OR linz_dvr_vacant_built.h3_index IS NOT NULL)
    AND water_features.h3_index IS NULL -- Eliminate any form of water from this class
    AND pan_nz_draft_h3.h3_index IS NULL -- Eliminate from consideration -anything- in PAN-NZ
);


-- \d9 land that is L = high confidence
-- NB : L --> Lifestyle land, generally in a rural area, where the predominant use is for a residence and, if vacant, there **is a right to build a dwelling.** (therefore this should be class 3)

-- Chch/Gisborne Red Zone in urban areas

-- actual use in rural (vacant) but category R or LV and zoned residential - very high confidence

-- TODO hail (contaminated brownfield?)
