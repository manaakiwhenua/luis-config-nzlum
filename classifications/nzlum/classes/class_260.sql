CREATE TEMPORARY VIEW class_260 AS ( -- Intensive animal production
    SELECT h3_index,
    2 AS lu_code_primary,
    6 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN marine_farms.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            marine_farms.confidence,
            marine_farms.commod,
            marine_farms.manage,
            marine_farms.source_data,
            marine_farms.source_date,
            marine_farms.source_scale
        )::nzlum_type
        WHEN (
            linz_dvr_.actual_property_use IN ('0', '00', '01', '1', '10', '16')
            AND (
                linz_dvr_.category ~ '^S[AHPSX]'
                OR linz_dvr_.category ~ '^PS'
            )
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN (
                    linz_dvr_.improvements_description ~ '\m(ANML|ANIMAL)\s?(SHLTR|SHELTER)S?\M'
                    OR linz_dvr_.improvements_description ~ '\mSTABLES?\M'
                    OR linz_dvr_.improvements_description ~ '\mDEER\M'
                    OR linz_dvr_.improvements_description ~ '\mKENNELS?\M'
                )
                THEN 3
                WHEN linz_dvr_.category ~ '[AB]$'
                THEN 4 -- Higher confidence if a more economically viable property
                WHEN linz_dvr_.category ~ '[C]$'
                THEN 5
                WHEN linz_dvr_.zone !~ '^(0X|1|3|6)'
                THEN 9
                ELSE 6
            END, -- confidence
            ARRAY[
                CASE
                    WHEN linz_dvr_.category ~ '^SH' OR linz_dvr_.improvements_description ~ '\mSTABLES?\M'
                    THEN 'horses'
                    -- WHEN linz_dvr_.category ~ '^SP'
                    -- THEN 'poultry'
                    WHEN linz_dvr_.category ~ '^SS'
                    THEN 'pigs'
                    ELSE NULL
                END
            ]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM marine_farms
    FULL OUTER JOIN (
        SELECT *
        FROM linz_dvr_
        WHERE category ~ '^PS'
        OR category ~ '^S(A|H|P|S|X)' -- Specialist types except deer: aquaculture, horse studs and training operations, poultry, pigs, all other specialist livestock
        OR actual_property_use IN ('0', '00', '01', '1', '10', '16')
        -- TODO any other improvements codes that could be useful? PIGGERY?
    ) linz_dvr_ USING (h3_index)
);