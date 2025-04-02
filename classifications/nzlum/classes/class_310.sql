CREATE TEMPORARY VIEW class_310 AS ( -- Residential
    SELECT h3_index,
    3 AS lu_code_primary,
    1 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN hnz.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[hnz.source_data]::TEXT[],
            hnz.source_date,
            hnz.source_scale
        )::nzlum_type
        WHEN improvements_value > 0
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            GREATEST(LEAST(CASE
                WHEN (
                    actual_property_use LIKE '2%'
                    OR actual_property_use LIKE '9%'
                ) AND actual_property_use NOT LIKE '%9' -- Vacant
                THEN
                    CASE
                        WHEN gt_half_acre = TRUE THEN 3
                        ELSE 1
                    END
                WHEN actual_property_use LIKE '%0'  -- Multi-use within residential or lifestyle
                THEN 5
                WHEN category ~ '^(RA|RC|RD|RF|RH|RN|RR)'
                THEN 5
                WHEN (
                    actual_property_use LIKE '0%' -- Multi-use at primary level; secondary use residential or lifestyle
                    AND category LIKE 'R%'
                )
                THEN 8
                WHEN category ~ '^(RB|RM|RP|RV)'
                THEN 11
                ELSE 12
            END
            +
            CASE -- Adjustment factor is LINZ records as residential
                WHEN topo50_residential_areas_h3.h3_index IS NOT NULL
                THEN -1
                ELSE 0
            END, 12), 1),
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale
        )::nzlum_type
        WHEN actual_property_use LIKE '%9' -- Vacant
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary,
            11,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale
        )::nzlum_type
        WHEN (
                linz_dvr_.h3_index IS NOT NULL -- Residual housing
                AND (
                    irrigation_.status <> 'Current'
                    OR irrigation_.h3_index IS NULL
                ) -- Only allow residual housing where there is no current irrigation
            )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary,
            8,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[irrigation_.source_data, linz_dvr_.source_data]::TEXT[],
            range_merge(irrigation_.source_date,linz_dvr_.source_date),
            range_merge(irrigation_.source_scale,linz_dvr_.source_scale)
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM (
        SELECT * FROM linz_dvr_
        WHERE (
            actual_property_use ~ '^9' -- Residential
            OR actual_property_use ~ '^2' -- Lifestyle
            OR actual_property_use IN ('02', '09')
        ) OR (
            category ~ '^R' -- Residential
        )
    ) linz_dvr_
    LEFT JOIN irrigation_ USING (h3_index)
    FULL OUTER JOIN (
        SELECT *
        FROM linz_crosl_
        WHERE managed_by = 'Housing New Zealand'
    ) hnz USING (h3_index)
    FULL OUTER JOIN (
        SELECT *
        FROM topo50_residential_areas_h3
        WHERE :parent::h3index = h3_partition
    ) AS topo50_residential_areas_h3 USING (h3_index)
);