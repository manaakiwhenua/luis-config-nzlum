
-- 1.3.0 Minimal use from relatively natural environments

-- This class includes land that is subject to relatively low levels of intervention or that is largely unused in the context of prime use or use for resource protection. This land may be covered with indigenous or exotic plant species. It includes land where the structure of the native vegetation generally remains intact despite deliberate use, although the floristics of the vegetation may have changed markedly (e.g. grazing on native tussock land).

-- Where native grasses have been deliberately and extensively replaced with other species, the land use should not be classified under class 1.

-- 1.3.6 Defence land – natural areas allocated to field training, weapons testing, and other field defence uses, predominantly in rural areas (e.g. Kaipara Air Weapons Range and the Waiouru Military Camp). Areas associated with buildings or more built environments on defence land are captured under an urban class.

CREATE TEMPORARY VIEW class_136 AS ( 
    SELECT h3_index,
    1 AS lu_code_primary,
    3 AS lu_code_secondary,
    6 AS lu_code_tertiary,
    CASE
        WHEN (
            linz_crosl_nzdf.h3_index IS NOT NULL
            AND linz_dvr_.actual_property_use = '45' -- Defence
            AND lcdb_built_up.h3_index IS NULL
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[
                linz_crosl_nzdf.source_data,
                linz_dvr_.source_data
            ]::TEXT[],
            range_merge(
                linz_crosl_nzdf.source_date,
                linz_dvr_.source_date
            )::daterange,
            range_merge(
                linz_crosl_nzdf.source_scale,
                linz_dvr_.source_scale
            )::int4range,
            NULL
        )::nzlum_type
        WHEN linz_dvr_.actual_property_use = '45' -- Defence
        AND lcdb_built_up.h3_index IS NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale,
            NULL
        )::nzlum_type
        WHEN linz_crosl_nzdf.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN (
                    linz_dvr_.actual_property_use IN ('04', '40') -- Mixed use, including community services (which itself includes defence)
                    AND lcdb_built_up.h3_index IS NULL
                )
                THEN 1
                WHEN (
                    linz_dvr_.actual_property_use ~ '^5' -- Recreational
                    AND lcdb_built_up.h3_index IS NULL
                )
                THEN 2
                ELSE 5
            END,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY_REMOVE(ARRAY[linz_crosl_nzdf.source_data, linz_dvr_.source_data], NULL)::TEXT[],
            range_merge(datemultirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    linz_crosl_nzdf.source_date,
                    linz_dvr_.source_date
                ], NULL)
            ))::daterange, -- source_date
            range_merge(int4multirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    linz_crosl_nzdf.source_scale,
                    linz_dvr_.source_scale
                ], NULL)
            ))::int4range, -- source_scale
            NULL
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM (
        SELECT
        h3_index,
        source_data,
        source_date,
        source_scale
        FROM linz_crosl_
        WHERE managed_by = 'New Zealand Defence Force'
    ) AS linz_crosl_nzdf
    FULL OUTER JOIN linz_dvr_ USING (h3_index)
    LEFT JOIN ( -- Use to exclude or penalise built-up areas from this class
        SELECT * 
        FROM lcdb_
        WHERE Class_2018 NOT IN (
            1, -- 'Built-up Area (settlement)',
            2, -- 'Urban Parkland/Open Space'
            5 --'Transport Infrastructure',
        )
    ) AS lcdb_built_up USING (h3_index)
);

-- CROSL NZDF managed land
--      but confimed against actual use recorded in DVR where possible
--      in order to exclude e.g. NZDF residential land, parks in the same; but careful with actual use 53 (passive recreation, e.g. at Balmoral)
-- Also include DVR actual use 45 (defence)
-- Exclude from consideration built-up areas (settlement, urban parkland/open space, transport infrastructure) e.g. Ohakea airbase is not a natural area (though it does have some marginal land that is)

-- TODO no source for Kaipara Air Weapons Range