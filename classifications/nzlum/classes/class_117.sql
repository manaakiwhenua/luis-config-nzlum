CREATE TEMPORARY VIEW class_117 AS (
    SELECT h3_index,
    1 AS lu_code_primary,
    1 AS lu_code_secondary,
    7 AS lu_code_tertiary,
    CASE
        WHEN pan_nz_not_iucn.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            LEAST(GREATEST(
                CASE
                    WHEN (
                        legislation_act IN (
                            'WELLINGTON_TOWN_BELT_ACT_2016',
                            'LOCAL_GOVT_MANAGED_AREA'
                        )
                        OR (
                            legislation_act = 'RESERVES_ACT'
                            AND legislation_section = 'S23_LOCAL_PURPOSE_RESERVE'
                        )
                        OR (
                            legislation_act = 'LOCAL_GOVERNMENT_ACT'
                            AND legislation_section IN (
                                'S139_REGIONAL_PARK',
                                'S139_REGIONAL_PARKS'
                            )
                        )
                    ) 
                THEN 2
                WHEN (
                    legislation_act = 'LAND_ACT'
                    AND legislation_section = 'S52_BOARD_MAY_ALIENATE_CROWN_LAND'
                ) OR (
                    legislation_act = 'FOREST_ACT_REPEALED'
                    AND legislation_section = 'S18'
                ) OR (
                    legislation_act = 'PUBLIC_WORKS_ACT'
                ) OR (
                    legislation_act = 'RESERVES_ACT'
                    AND legislation_section IN (
                        'S17_RECREATION_RESERVE',
                        'S18_HISTORIC_RESERVE'
                    ) 
                ) OR (
                    legislation_section = 'MAORI_RESERVATION'
                ) OR (
                    
                    legislation_act = 'RESOURCE_MANAGEMENT_ACT'
                    AND legislation_section IN (
                        'S6', -- Significant Natural Areas
                        'S221_CONSENT_NOTICE'
                    )
                ) OR (
                    legislation_act = 'RIVER_BOARDS_ACT'
                    AND legislation_section = 'S73_RIVER_BED'
                )
                THEN 12
                WHEN designation IN (
                    'Amenities Area', 'Amenity Area',
                    'Ambiguous',
                    'Dog Area',
                    'Recreation Reserve',
                    'Recreational Reserve - Racecourse',
                    'Recreational Hunting Area',
                    'River Bed',
                    'Road Reserve',
                    'Water Supply', 'Water Supply and Recreation Purposes', 'Water Supply Reserve',
                    'Consent Notice'
                )
                THEN 7
                ELSE 1
                -- Penalise any form of urban area
                + CASE WHEN rural.h3_index IS NULL THEN 1 ELSE 0 END
                -- Penalise built land-covder including urban parks
                + CASE WHEN lcdb_unbuilt.h3_index IS NULL THEN 1 ELSE 0 END
            END, 1), 12),
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[pan_nz_not_iucn.source_data],
            pan_nz_not_iucn.source_date,
            pan_nz_not_iucn.source_scale,
            pan_nz_not_iucn.source_protection_name
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM (
        -- Deal with overlaps
        SELECT DISTINCT ON (h3_index)
        h3_index,
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
            'Not Mapped',
            'Not IUCN'
        )
        AND NOT (
            legislation_act = 'RESERVES_ACT'
            AND legislation_section = 'S16_11_RECREATION_RESERVE_RACECOURSE'
        ) -- Racecourses included under 3.2.0
        ORDER BY
            h3_index,
            source_date DESC NULLS LAST, -- Prefer more recent
            source_id -- Tie-break
    ) pan_nz_not_iucn
    LEFT JOIN (
        SELECT h3_index
        FROM lcdb_
        WHERE Class_2018 NOT IN (
            1, -- Settlement
            2, -- Urban parkland open space
            5 -- Transport infrastructure
        )
    ) AS lcdb_unbuilt USING (h3_index)
    LEFT JOIN (
        SELECT h3_index
        FROM urban_rural_2025
        JOIN urban_rural_2025_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND urban_rural_2025.IUR2025_V1_00 IN (
            '22', -- Rural other
            '31', -- Inland water
            '32', -- Inlet
            '33' -- Oceanic
        )
    ) AS rural USING (h3_index)
)