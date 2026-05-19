CREATE TEMPORARY VIEW class_120 AS (
    SELECT roi.h3_index,
    1 AS lu_code_primary,
    2 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN pan_nz_historic_reserves.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE WHEN lcdb_unbuilt.h3_index IS NULL THEN 1 ELSE 4 END,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[pan_nz_historic_reserves.source_data],
            pan_nz_historic_reserves.source_date,
            pan_nz_historic_reserves.source_scale,
            pan_nz_historic_reserves.source_protection_name
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM roi
    JOIN (
        -- Deal with overlaps
        SELECT DISTINCT ON (pan_nz_draft_h3.h3_index)
        pan_nz_draft_h3.h3_index,
        source_data,
        source_date,
        source_scale,
        source_protection_name
        FROM pan_nz_draft
        JOIN pan_nz_draft_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND (
            legislation_act = 'BURIAL_GROUND_MASSEY_BURIAL_GROUND_ACT_1925'
        ) OR (
            legislation_act = 'TE_TURE_WHENUA_MAORI_ACT'
            AND legislation_section = 'MAORI_RESERVATION'
        ) OR (
            legislation_act = 'Section 29 Conservation Act'
            AND legislation_section = 'MAORI_RESERVATION'
        ) OR (
            legislation_act = 'Section 77 Reserves Act 1977'
            AND legislation_section = 'MAORI_RESERVATION'
        ) OR (
            legislation_act = 'Section 77A Reserves Act 1977'
            AND legislation_section = 'MAORI_RESERVATION'
        ) OR (
            legislation_act = 'RESERVES_ACT'
            AND legislation_section = 'S18_HISTORIC_RESERVE'
        )
        ORDER BY
            pan_nz_draft_h3.h3_index,
            source_date DESC NULLS LAST, -- Prefer more recent
            source_id -- Tie-break
    ) pan_nz_historic_reserves ON roi.h3_index && pan_nz_historic_reserves.h3_index
    LEFT JOIN (
        SELECT h3_index
        FROM lcdb_
        WHERE Class_2018 NOT IN (
            1, -- Settlement
            2, -- Urban parkland open space
            5 -- Transport infrastructure
        )
    ) AS lcdb_unbuilt ON roi.h3_index && lcdb_unbuilt.h3_index
)