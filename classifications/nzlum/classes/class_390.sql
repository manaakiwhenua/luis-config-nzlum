-- Vacant and transitioning land

-- This land-use type includes areas that are currently unused or undergoing a transition from one land use to another, but in this case with a clear transition towards or within other concepts under the built environment.

--     Vacant land – includes derelict land and developed land that is idle.

--     Greenfield development – previously undeveloped or agricultural land zones for or undergoing new construction projects or urban expansion, typically involving the conversion of rural or natural areas into residential, commercial, industrial, or infrastructural uses.

--     Brownfield development – areas of active redevelopment of previously developed (often industrial) land that may be abandoned, contaminated, or economically under-utilised, with the aim of rehabilitating and repurposing these sites for new urban activities. May include residential areas undergoing infill development that increase housing density.

CREATE TEMPORARY VIEW class_390 AS (
    SELECT h3_index,
    3 AS lu_code_primary,
    9 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN 
            transitional_land.h3_index IS NOT NULL
            AND urban_rural_2025_.h3_index IS NOT NULL
        THEN CASE
            WHEN linz_dvr_full_.h3_index IS NULL
            THEN ROW(
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
                linz_dvr_full_.actual_property_use ~ '[1-9]9$' -- Vacant
                OR linz_dvr_full_.actual_property_use IN (
                    '00', -- Vacant
                    '55' -- Passive outdoor
                )
                OR linz_dvr_full_.actual_property_use IS NULL
            )
            THEN ROW(
                ARRAY[]::TEXT[], -- lu_code_ancillary
                1,
                ARRAY[]::TEXT[], -- commod
                ARRAY[]::TEXT[], -- manage
                ARRAY[transitional_land.source_data, linz_dvr_full_.source_data]::TEXT[],
                range_merge(transitional_land.source_date, linz_dvr_full_.source_date),
                range_merge(transitional_land.source_scale, linz_dvr_full_.source_scale),
                NULL
            )::nzlum_type
            WHEN linz_dvr_full_.category ~ '\wV'
            THEN ROW(
                ARRAY[]::TEXT[], -- lu_code_ancillary
                CASE
                    WHEN linz_dvr_full_.zone ~ '^[789]' -- Higher confidence when also zoned for industrial, commercial, residential
                    THEN 8
                    ELSE 10
                END,
                ARRAY[]::TEXT[], -- commod
                ARRAY[]::TEXT[], -- manage
                ARRAY[transitional_land.source_data, linz_dvr_full_.source_data]::TEXT[],
                range_merge(
                    transitional_land.source_date,
                    linz_dvr_full_.source_date
                ),
                range_merge(
                    transitional_land.source_scale,
                    linz_dvr_full_.source_scale
                ),
                NULL
            )::nzlum_type
            END
        WHEN linz_dvr_vacant_built.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN linz_dvr_vacant_built.actual_property_use IN (
                    '39', -- Transport: vacant
                    '49', -- Community services: vacant
                    '59', -- Recreational: vacant
                    '69', -- Utility services: vacant
                    '79', -- Industrial: vacant
                    '89', -- Commercial: vacant
                    '99' -- Residetial: vacant
                )
                THEN
                    CASE
                        WHEN linz_dvr_vacant_built.zone ~ '^[24789].*' AND low_ratio_improvements
                        THEN 1
                        WHEN linz_dvr_vacant_built.zone ~ '^[24789].*'
                        THEN 2
                        ELSE 5
                    END
                WHEN linz_dvr_vacant_built.actual_property_use IN (
                    '00', -- Multi-use at primary level: Vacant or intermediate
                    '29' -- Lifestyle: vacant
                ) THEN
                    CASE
                        WHEN linz_dvr_vacant_built.zone ~ '^[24789].*' AND low_ratio_improvements
                        THEN 3
                        WHEN linz_dvr_vacant_built.zone ~ '^[24789].*'
                        THEN 4
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
            END, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_vacant_built.source_data]::TEXT[],
            linz_dvr_vacant_built.source_date,
            linz_dvr_vacant_built.source_scale,
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
    ) AS linz_dvr_vacant_built
    FULL OUTER JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale,
            actual_property_use,
            category,
            "zone"
        FROM linz_dvr_
    ) AS linz_dvr_full_ USING (h3_index)
    FULL OUTER JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale
        FROM transitional_land
    ) AS transitional_land USING (h3_index)
    LEFT JOIN (
        SELECT
            h3_index,
            IUR2025_V1_00
        FROM urban_rural_2025_h3
        JOIN urban_rural_2025 USING (ogc_fid)
        WHERE
            :parent::h3index = h3_partition
            AND urban_rural_2025.IUR2025_V1_00 <> '22' -- (non) "Rural other"
    ) AS urban_rural_2025_ USING (h3_index)
    LEFT JOIN water_features USING (h3_index)
    LEFT JOIN pan_nz_draft_h3 USING (h3_index)
    WHERE water_features.h3_index IS NULL -- Eliminate any form of water from this class
    AND pan_nz_draft_h3.h3_index IS NULL -- Eliminate from consideration -anything- in PAN-NZ
);


-- \d9 land that is L = high confidence
-- NB : L --> Lifestyle land, generally in a rural area, where the predominant use is for a residence and, if vacant, there **is a right to build a dwelling.** (therefore this should be class 3)

-- Chch/Gisborne Red Zone in urban areas

-- actual use in rural (vacant) but category R or LV and zoned residential - very high confidence

-- TODO hail (contaminated brownfield?)
