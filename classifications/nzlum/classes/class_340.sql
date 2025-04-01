CREATE TEMPORARY VIEW class_340 AS ( -- Manufacturing and industrial
    SELECT h3_index,
    3 AS lu_code_primary,
    4 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    ROW(
        ARRAY[]::TEXT[], -- lu_code_ancillary
        CASE
            WHEN
                actual_property_use <> '07'
                AND actual_property_use NOT LIKE '%0'
                AND improvements_value > 0
            THEN 2
            WHEN actual_property_use = '70' -- Multi-use within industrial
            THEN
                CASE
                    WHEN improvements_value > 0
                    THEN 2
                    ELSE 5
                END
            WHEN
                (
                    actual_property_use LIKE '79' -- Vacant
                    OR actual_property_use = '07'
                )
                AND (
                    improvements_description IS NULL
                    OR improvements_value = 0
                )
            THEN 11
            ELSE 8
        END,
        ARRAY[]::TEXT[],
        ARRAY[]::TEXT[],
        ARRAY[source_data]::TEXT[],
        source_date,
        source_scale
    )::nzlum_type AS nzlum_type
    FROM (
        SELECT * FROM linz_dvr_
        WHERE (
            actual_property_use LIKE '7%'
            OR actual_property_use = '07'
        ) AND NOT actual_property_use = '78' -- Depots and yards; considered commercial not industrial
    )
);