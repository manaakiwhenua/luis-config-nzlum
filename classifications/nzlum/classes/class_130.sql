CREATE TEMPORARY VIEW class_130 AS (
    SELECT h3_index,
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
            pan_nz_draft_.source_scale
        )::nzlum_type
        WHEN high_country_leases.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            2, -- Some uncertainty due to indicative boundaries
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[high_country_leases.source_data],
            high_country_leases.source_date,
            high_country_leases.source_scale
        )::nzlum_type 
        ELSE NULL
    END AS nzlum_type
    FROM (
        -- Deal with overlaps
        SELECT DISTINCT ON (h3_index)
        h3_index,
        source_data,
        source_date,
        source_scale
        FROM pan_nz_draft
        JOIN pan_nz_draft_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND legislation_act = 'RESERVES_ACT'
        AND designation IN (
            'Water Supply',
            'Water Supply and Recreation Purposes',
            'Water Supply Reserve'
        )
        ORDER BY
            h3_index,
            source_date DESC NULLS LAST, -- Prefer more recent
            source_id -- Tie-break
    ) pan_nz_draft_
    FULL OUTER JOIN (
        SELECT h3_index,
        'LINZ' AS source_data,
        daterange(
            '2013-02-07'::DATE,
            '2024-11-04'::DATE,
            '[]'
        ) AS source_date,
        '(100,)'::int4range AS source_scale -- Boundaries are indicative only
        FROM south_island_pastoral_leases_h3
        INNER JOIN lcdb_ USING (h3_index)
        WHERE :parent::h3index = h3_partition
        AND Class_2018 NOT IN (
            '1', -- Settlement
            '2', -- Urban parkland
            '5', -- Transport infrastructure
            '6', -- Surface mine or dump
            '30', -- Cropland
            '33', -- Orchards etc.
            '40', -- High Producing Exotic Grassland
            '64', -- Forested - Harvested
            '68', -- Deciduous hardwoods
            '71' -- Exotic forest
        ) -- Exclude obvious non-natural land covers
    ) AS high_country_leases USING (h3_index)  
);