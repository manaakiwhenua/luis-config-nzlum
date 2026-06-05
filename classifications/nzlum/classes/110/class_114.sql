CREATE TEMPORARY VIEW class_114 AS (
    SELECT roi.h3_index,
    1 AS lu_code_primary,
    1 AS lu_code_secondary,
    4 AS lu_code_tertiary,
    CASE
        WHEN pan_nz_draft_iucn_III.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[pan_nz_draft_iucn_III.source_data],
            pan_nz_draft_iucn_III.source_date,
            pan_nz_draft_iucn_III.source_scale,
            ARRAY_TO_STRING(
                ARRAY_REMOVE(
                    ARRAY[
                        pan_nz_draft_iucn_III.designation,
                        NULLIF(CONCAT_WS(' ', pan_nz_draft_iucn_III.legislation_act, pan_nz_draft_iucn_III.legislation_section), ''),
                        pan_nz_draft_iucn_III.source_protection_name
                    ],
                    NULL
                ),
                E'\n'
            )
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM roi
    JOIN (
        -- Deal with overlaps
        SELECT DISTINCT ON (h3_index)
        h3_index,
        source_data,
        source_date,
        source_scale,
        legislation_act,
        legislation_section,
        designation,
        source_protection_name
        FROM pan_nz_draft
        JOIN pan_nz_draft_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND iucn_category = 'III'
        ORDER BY
            h3_index,
            source_date DESC NULLS LAST, -- Prefer more recent
            source_id -- Tie-break
    ) pan_nz_draft_iucn_III ON roi.h3_index && pan_nz_draft_iucn_III.h3_index
)