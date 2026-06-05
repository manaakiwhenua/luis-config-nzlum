CREATE TEMPORARY VIEW class_117 AS (
    SELECT roi.h3_index,
    1 AS lu_code_primary,
    1 AS lu_code_secondary,
    7 AS lu_code_tertiary,
    CASE
        WHEN pan_nz_not_iucn.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            clamp_confidence_or_null(
                CASE
                    WHEN (
                        legislation_act = 'Wellington Town Belt Act 2016'
                        OR (
                            legislation_act = 'Reserves Act 1977'
                            AND legislation_section = 'S.23' -- Local Purpose Reserve
                        )
                        OR (
                            legislation_act = 'Local Government Act 2022'
                            AND legislation_section = 'S.139' -- Regional Park
                        )
                    ) 
                THEN 2
                WHEN (
                    legislation_act = 'Reserves Act 1977'
                    AND legislation_section IN (
                        'S.17', -- Recreation Reserve,
                        'S.18' -- Historic Reserve
                    )
                )
                THEN 3
                WHEN (
                    legislation_act = 'Land Act 1948'
                    AND legislation_section IN (
                        'S.52', -- Crown land alienation
                        'S.176(1)(a)' -- Unoccupied Crown land
                    ) 
                ) OR (
                    legislation_act = 'Public Works Act 1981'
                    AND legislation_section IN (
                        'S.20', -- Acquired for Public Works
                        'S.52' -- Land Held for a Government Work
                    )
                ) OR (
                    designation = 'Maori Reservation'
                ) OR (
                    legislation_act = 'Resource Management Act 1991'
                    AND legislation_section IN (
                        'S.6', -- Significant Natural Areas
                        'S.221' -- Consent Notice
                    )
                ) OR (
                    legislation_act = 'River Boards Act 1908'
                    AND legislation_section = 'S.73' -- Riverbed and Lakebed Protection
                )
                THEN 10
                WHEN designation IN (
                    'Amenities Area',
                    'Dog Area',
                    'Recreational Hunting Area',
                    'Legal Road and Road Reserve', -- Paper roads
                    'Water Conservation Reserve'
                )
                THEN 5
                ELSE 1
            END),
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[pan_nz_not_iucn.source_data],
            pan_nz_not_iucn.source_date,
            pan_nz_not_iucn.source_scale,
            ARRAY_TO_STRING(
                ARRAY_REMOVE(
                    ARRAY[
                        pan_nz_not_iucn.designation,
                        NULLIF(CONCAT_WS(' ', pan_nz_not_iucn.legislation_act, pan_nz_not_iucn.legislation_section), ''),
                        pan_nz_not_iucn.source_protection_name
                    ],
                    NULL
                ),
                E'\n'
            )
        )::nzlum_type
        -- Reserve-designated land (DVR zone '0*') with native cover but absent from PAN-NZ.
        WHEN (
            linz_dvr_reserve.h3_index IS NOT NULL
            AND lcdb_native.h3_index IS NOT NULL
            AND pan_nz_not_iucn.h3_index IS NULL
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            6, -- DVR reserve zone + native LCDB cover, without PAN-NZ backing
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_reserve.source_data]::TEXT[],
            linz_dvr_reserve.source_date,
            linz_dvr_reserve.source_scale,
            NULL
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM roi
    LEFT JOIN (
        -- Deal with overlaps
        SELECT DISTINCT ON (pan_nz_draft_h3.h3_index)
        pan_nz_draft_h3.h3_index,
        legislation_act,
        legislation_section,
        designation,
        source_data,
        source_date,
        source_scale,
        source_protection_name
        FROM pan_nz_draft
        JOIN pan_nz_draft_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND iucn_category IN (
            'VI',
            'Not Mapped',
            'Not IUCN'
        )
        AND designation NOT IN (
            'Racecourse',         -- Racecourses included under 3.2.1
            'Significant Natural Area',
            'Ramsar List site',
            'World Heritage Area', -- UNESCO World Heritage
            'Crown Pastoral Lease' -- Extensive pastoral tenure → class 2.2.3
        )
        ORDER BY
            pan_nz_draft_h3.h3_index,
            CASE WHEN iucn_category = 'Not IUCN' THEN 1
                 WHEN iucn_category = 'Not Mapped' THEN 2
                 ELSE 3 -- VI last as most likely to be genuine protected land
            END DESC, -- Custom priority for this class, prefer higher number
            priority_rank ASC NULLS LAST,
            source_date DESC NULLS LAST, -- Prefer more recent
            source_id -- Tie-break if still necessary
    ) pan_nz_not_iucn ON roi.h3_index && pan_nz_not_iucn.h3_index
    -- Detect LCDB coverage (any class) — to distinguish "non-native" from "no data"
    LEFT JOIN lcdb_ AS lcdb_any ON roi.h3_index && lcdb_any.h3_index
    -- Detect indigenous cover specifically
    LEFT JOIN (
        SELECT h3_index FROM lcdb_
        WHERE Class_2023 NOT IN (
            1, -- Built-up area
            2, -- Urban parkland/Open space
            5, -- Transport infrastructure
            6, -- Surface mine or dump
            30, -- Short-rotation cropland
            33, -- Orchards, Vineyards or Other Perennial Crops
            40, -- High Producing Exotic Grassland
            41, -- Low-producing grassland
            64, -- Forest - Harvested
            71 -- Exotic forest
        )
    ) AS lcdb_native ON roi.h3_index && lcdb_native.h3_index
    -- DVR reserve-designated zone (zone code '0*', excluding '0X' multi-zone).
    -- Used as a fallback path for reserve land absent from PAN-NZ.
    LEFT JOIN (
        SELECT h3_index, source_data, source_date, source_scale
        FROM linz_dvr_
        WHERE "zone" LIKE '0%'
        AND "zone" != '0X'
    ) AS linz_dvr_reserve ON roi.h3_index && linz_dvr_reserve.h3_index
    WHERE (
        -- PAN-NZ path: keep where native cover is present or LCDB has no data
        pan_nz_not_iucn.h3_index IS NOT NULL
        AND (lcdb_native.h3_index IS NOT NULL OR lcdb_any.h3_index IS NULL)
    ) OR (
        -- DVR reserve zone path: reserve-designated with native cover, absent from PAN-NZ
        linz_dvr_reserve.h3_index IS NOT NULL
        AND lcdb_native.h3_index IS NOT NULL
        AND pan_nz_not_iucn.h3_index IS NULL
    )
)

-- TODO crown pastoral lease to class 2.2.3?