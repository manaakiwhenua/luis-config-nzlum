CREATE TEMPORARY VIEW class_140 AS ( -- Unused land and land in transition
    SELECT h3_index,
    1 AS lu_code_primary,
    4 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN pan_nz_public_works.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            8, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[pan_nz_public_works.source_data]::TEXT[],
            pan_nz_public_works.source_date,
            pan_nz_public_works.source_scale,
            pan_nz_public_works.source_protection_name
        )::nzlum_type
        WHEN linz_dvr_vacant_other IS NOT NULL
        THEN ROW (
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN linz_dvr_vacant_other.improvements_value > 100000
                THEN 8
                WHEN category ~ '^0' -- Designated or zoned reserve land
                THEN 4
                ELSE 6
            END, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_vacant_other.source_data]::TEXT[],
            linz_dvr_vacant_other.source_date,
            linz_dvr_vacant_other.source_scale,
            NULL
        )::nzlum_type
        WHEN ecan_braided_rivers_.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            4, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[ecan_braided_rivers_.source_data]::TEXT[],
            ecan_braided_rivers_.source_date,
            ecan_braided_rivers_.source_scale,
            NULL
        )::nzlum_type
        WHEN topo50_rocks_polygons.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            12, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[topo50_rocks_polygons.source_data]::TEXT[],
            topo50_rocks_polygons.source_date,
            topo50_rocks_polygons.source_scale,
            NULL
        )::nzlum_type
        WHEN topo50_scree.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            12, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[topo50_scree.source_data]::TEXT[],
            topo50_scree.source_date,
            topo50_scree.source_scale,
            NULL
        )::nzlum_type
        WHEN topo50_snow.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            12, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[topo50_snow.source_data]::TEXT[],
            topo50_snow.source_date,
            topo50_snow.source_scale,
            NULL
        )::nzlum_type
        WHEN topo50_moraine_polygons.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            12, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[topo50_moraine_polygons.source_data]::TEXT[],
            topo50_moraine_polygons.source_date,
            topo50_moraine_polygons.source_scale,
            NULL
        )::nzlum_type
        WHEN topo50_moraine_wall_polygons.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            12, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[topo50_moraine_wall_polygons.source_data]::TEXT[],
            topo50_moraine_wall_polygons.source_date,
            topo50_moraine_wall_polygons.source_scale,
            NULL
        )::nzlum_type
        WHEN topo50_shingle_polygons.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            12, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[topo50_shingle_polygons.source_data]::TEXT[],
            topo50_shingle_polygons.source_date,
            topo50_shingle_polygons.source_scale,
            NULL
        )::nzlum_type
        WHEN lcdb_.class_2018 IN (
            10, -- Sand or Gravel
            12, -- Landslide
            14, -- Permanent Snow and Ice
            15, -- Alpine Grass/Herbfield
            16, -- Gravel or Rock
            21, -- River
            43, -- Tall Tussock Grassland (Indigenous snow tussocks in mainly alpine mountain-lands and red tussock in the central North Island and locally in poorly-drained valley floors, terraces and basins of both islands.)
            44, -- Depleted Grassland
            45, -- Herbaceous Freshwater Vegetation 
            46, -- Herbaceous Saline Vegetation
            47, -- Flaxland
            50, -- Fernland
            51, -- Gorse and/or Broom
            52, -- Manuka and/or Kanuka
            54, -- Broadleaved Indigenous Hardwoods
            55, -- Sub Alpine Shrubland
            56, -- Mixed Exotic Shrubland
            58, -- Matagouri or Grey Scrub
            68, -- Deciduous Hardwoods
            69, -- Indigenous Forest
            70, -- Mangrove
            80, -- Peat Shrubland (Chatham Is)
            81 -- Dune Shrubland (Chatham Is)
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            12, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[lcdb_.source_data]::TEXT[],
            lcdb_.source_date,
            lcdb_.source_scale,
            NULL
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM (
        SELECT h3_index,
        daterange(
            createddate::date,
            modifieddate::date,
            '[]'
        ) AS source_date,
        -- TODO source_scale using the sourcecode and QA codes
        '(0,1]'::int4range AS source_scale,
        'ECAN' AS source_data
        FROM ecan_braided_rivers_h3
        JOIN ecan_braided_rivers USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS ecan_braided_rivers_
    FULL OUTER JOIN (
        SELECT h3_index,
        improvements_value,
        category,
        source_data,
        source_date,
        source_scale
        FROM linz_dvr_
        WHERE category = 'OV' -- Other vacnt
        AND (
            actual_property_use = '00' -- Multi-use at primary level: vacant or intermediate
            OR actual_property_use ~ '[1-9]0' -- All kinds of vacant
        )
    ) linz_dvr_vacant_other USING (h3_index)
    FULL OUTER JOIN lcdb_ USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,        
        'LINZ' AS source_data,
        daterange(
            '2011-05-22'::date,
            '2025-01-03'::date,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale
        FROM topo50_rocks_polygons_h3
        WHERE :parent::h3index = h3_partition

        UNION ALL

        SELECT h3_index,
        'LINZ' AS source_data,
        daterange(
            '2011-05-22'::date,
            '2024-03-20'::date,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale
        FROM topo50_chatham_rocks_polygons_h3
        WHERE :parent::h3index = h3_partition
    ) AS topo50_rocks_polygons USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,
        'LINZ' AS source_data,
        daterange(
            '2011-05-22'::date,
            '2025-01-03-'::date,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale
        FROM topo50_scree_polygons_h3
        WHERE :parent::h3index = h3_partition
    ) topo50_scree USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,
        'LINZ' AS source_data,
        daterange(
            '2011-05-22'::date,
            '2025-01-03'::date,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale
        FROM topo50_snow_polygons_h3
        WHERE :parent::h3index = h3_partition
    ) topo50_snow USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,
        'LINZ' AS source_data,
        daterange(
            '2011-05-22'::date,
            '2025-03-01'::date,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale
        FROM topo50_moraine_polygons_h3
        WHERE :parent::h3index = h3_partition
    ) topo50_moraine_polygons USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,
        'LINZ' AS source_data,
        daterange(
            '2011-05-22'::date,
            '2025-01-03'::date,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale
        FROM topo50_moraine_wall_polygons_h3
        WHERE :parent::h3index = h3_partition
    ) topo50_moraine_wall_polygons USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,        
        'LINZ' AS source_data,
        daterange(
            '2011-05-22'::date,
            '2025-01-03'::date,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale
        FROM topo50_shingle_polygons_h3
        WHERE :parent::h3index = h3_partition

        UNION ALL

        SELECT h3_index,
        'LINZ' AS source_data,
        daterange(
            '2011-05-22'::date,
            '2024-05-20'::date,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale
        FROM topo50_chatham_shingle_polygons_h3
        WHERE :parent::h3index = h3_partition
    ) AS topo50_shingle_polygons USING (h3_index)
    FULL OUTER JOIN (
        SELECT  DISTINCT ON (h3_index)
        h3_index,
        source_data,
        source_date,
        source_scale,
        source_protection_name
        FROM pan_nz_draft
        JOIN pan_nz_draft_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND (
            legislation_act = 'RESERVES_ACT'
            AND (
                legislation_section IS NULL 
                OR legislation_section = 'Acquired for Public Works'
            )
        )
        ORDER BY
            h3_index,
            source_date DESC NULLS LAST, -- Prefer more recent
            source_id -- Tie-break
    ) AS pan_nz_public_works USING (h3_index)
)

-- DVR: "category" = 'OV' AND "actual_property_use" LIKE '%9'
-- LOW confidence with some capital value
-- Perhaps only high confidence if zone is '0%' (designated or zoned reserve land)?

-- topo50 rocks
-- NB rock points? https://data.linz.govt.nz/layer/50331-nz-rock-points-topo-150k/
-- NB rock outcrops?: https://data.linz.govt.nz/layer/50330-nz-rock-outcrop-points-topo-150k/
-- scree
-- snow
-- moraines, moraine walls

-- Relevant LCDB classes