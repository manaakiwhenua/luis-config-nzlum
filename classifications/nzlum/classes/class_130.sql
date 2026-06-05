CREATE TEMPORARY VIEW class_130 AS (
    SELECT roi.h3_index,
    1 AS lu_code_primary,
    3 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN pan_nz_draft_.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[pan_nz_draft_.source_data],
            pan_nz_draft_.source_date,
            pan_nz_draft_.source_scale,
            pan_nz_draft_.source_protection_name
        )::nzlum_type
        WHEN high_country_leases.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            2, -- Some uncertainty due to indicative boundaries
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[high_country_leases.source_data],
            high_country_leases.source_date,
            high_country_leases.source_scale,
            NULL
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM roi
    LEFT JOIN (
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
        AND legislation_act = 'Reserves Act 1977'
        AND designation = 'Water Supply Reserve'
        ORDER BY
            pan_nz_draft_h3.h3_index,
            source_date DESC NULLS LAST, -- Prefer more recent
            source_id -- Tie-break
    ) pan_nz_draft_ ON roi.h3_index && pan_nz_draft_.h3_index
    LEFT JOIN (
        SELECT south_island_pastoral_leases_h3.h3_index,
        'LINZ' AS source_data,
        daterange(
            '2013-02-07'::DATE,
            '2024-11-04'::DATE,
            '[]'
        ) AS source_date,
        '[100,)'::int4range AS source_scale -- Boundaries are indicative only
        FROM south_island_pastoral_leases_h3
        WHERE :parent::h3index = south_island_pastoral_leases_h3.h3_partition
        AND EXISTS (
            SELECT 1 FROM lcdb_
            WHERE :parent::h3index = h3_partition
            AND Class_2023 NOT IN (
                1, -- Settlement
                2, -- Urban parkland
                5, -- Transport infrastructure
                6, -- Surface mine or dump
                30, -- Cropland
                33, -- Orchards etc.
                40, -- High Producing Exotic Grassland
                44, -- Depleted Grassland — in pastoral lease context signals grazing impact (→ 2.2.3)
                64, -- Forested - Harvested
                68, -- Deciduous hardwoods
                71 -- Exotic forest
            ) -- Exclude obvious non-natural land covers
            AND south_island_pastoral_leases_h3.h3_index && lcdb_.h3_index
        )
    ) AS high_country_leases ON roi.h3_index && high_country_leases.h3_index
    WHERE pan_nz_draft_.h3_index IS NOT NULL OR high_country_leases.h3_index IS NOT NULL
);