CREATE TEMPORARY VIEW class_116 AS (
    SELECT h3_index,
    1 AS lu_code_primary,
    1 AS lu_code_secondary,
    6 AS lu_code_tertiary,
    CASE
        WHEN pan_nz_draft_iucn_V.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[pan_nz_draft_iucn_V.source_data],
            pan_nz_draft_iucn_V.source_date,
            pan_nz_draft_iucn_V.source_scale,
            pan_nz_draft_iucn_V.source_protection_name
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM (
        -- Deal with overlaps
        SELECT DISTINCT ON (h3_index)
        h3_index,
        source_data,
        source_date,
        source_scale,
        source_protection_name
        FROM pan_nz_draft
        JOIN pan_nz_draft_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND iucn_category = 'V'
        ORDER BY
            h3_index,
            source_date DESC NULLS LAST, -- Prefer more recent
            source_id -- Tie-break
    ) pan_nz_draft_iucn_V
)