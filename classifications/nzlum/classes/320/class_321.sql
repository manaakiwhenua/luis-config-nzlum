-- 3.2.1 Outdoor recreation
-- Land areas dedicated to leisure activities conducted in natural or semi-natural settings,
-- typically with considerable unsealed vegetated areas. Includes parks, trails, sportsgrounds,
-- camping grounds, zoos, botanic gardens, and recreational reserves.
--
-- Scope note: Parks or reserves with a high level of native bush, or that are protected areas,
-- should be classified under class 1. This class captures the complement: recreational reserves
-- and open space where land cover is non-native or where the primary purpose is recreation
-- rather than conservation.
--
-- Scope note: This category may be used to identify developed recreational areas that fall
-- outside urban boundaries, such as mountain-bike parks.
--
-- Sources:
--   topo50_sportsfields / topo50_golf_courses: purpose-built outdoor recreation facilities
--   hail (C2): gun clubs and rifle ranges
--   pan_nz_draft_racecourses: racecourse reserves
--   lcdb_ (Urban Parkland/Open Space): satellite-derived open space in urban areas
--   pan_nz_recreational: PAN-NZ reserves (IUCN VI, Not Mapped, Not IUCN) where LCDB shows
--     non-native cover — the complement of class 1.1.7 which retains indigenous-cover reserves
--   dvr_public_rec_and_services: DVR properties with outdoor recreation improvements

CREATE TEMPORARY VIEW class_321 AS (
    SELECT roi.h3_index,
    3 AS lu_code_primary,
    2 AS lu_code_secondary,
    1 AS lu_code_tertiary,
    CASE
        WHEN recreational_ponds.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[recreational_ponds.source_data]::TEXT[],
            recreational_ponds.source_date,
            recreational_ponds.source_scale,
            NULL
        )::nzlum_type
        WHEN (
            golf_courses.h3_index IS NOT NULL AND dvr_public_rec_and_services.h3_index IS NOT NULL
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[golf_courses.source_data, dvr_public_rec_and_services.source_data]::TEXT[],
            range_merge(datemultirange(VARIADIC ARRAY_REMOVE(ARRAY[
                golf_courses.source_date,
                dvr_public_rec_and_services.source_date
            ], NULL)))::daterange,
            range_merge(int4multirange(VARIADIC ARRAY_REMOVE(ARRAY[
                golf_courses.source_scale,
                dvr_public_rec_and_services.source_scale
            ], NULL)))::int4range,
            NULL
        )::nzlum_type
        WHEN sportsfields.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[sportsfields.source_data]::TEXT[],
            sportsfields.source_date,
            sportsfields.source_scale,
            NULL
        )::nzlum_type
        WHEN hail_gun_clubs.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN hail_gun_clubs.hail_category_count = 1 THEN 1
                ELSE 4
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[hail_gun_clubs.source_data]::TEXT[],
            hail_gun_clubs.source_date,
            hail_gun_clubs.source_scale,
            NULL
        )::nzlum_type
        -- PAN-NZ reserves without indigenous cover: complement of class_117.
        -- class_117 keeps these when LCDB shows native cover; this class picks them up when it doesn't.
        WHEN (
            pan_nz_recreational.h3_index IS NOT NULL
            AND lcdb_native.h3_index IS NULL
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            -- Mirrors class_117 confidence levels for the same legislation/designation signals
            clamp_confidence_or_null(CASE
                WHEN (
                    pan_nz_recreational.legislation_act = 'Wellington Town Belt Act 2016'
                    OR (
                        pan_nz_recreational.legislation_act = 'Reserves Act 1977'
                        AND pan_nz_recreational.legislation_section = 'S.23'
                    )
                    OR (
                        pan_nz_recreational.legislation_act = 'Local Government Act 2022'
                        AND pan_nz_recreational.legislation_section = 'S.139'
                    )
                ) THEN 2
                WHEN (
                    pan_nz_recreational.legislation_act = 'Reserves Act 1977'
                    AND pan_nz_recreational.legislation_section IN ('S.17', 'S.18')
                ) THEN 3
                WHEN pan_nz_recreational.designation IN (
                    'Amenities Area', 'Dog Area', 'Recreational Hunting Area',
                    'Legal Road and Road Reserve', 'Water Conservation Reserve'
                ) THEN 5
                ELSE 1
            END
            + CASE -- Reserves without native cover in rural areas are more ambiguous
                WHEN urban_rural_.IUR2026_V1_00 IN ('11','12','13','14') THEN 0 -- Urban: confirms public recreational use
                WHEN urban_rural_.IUR2026_V1_00 = '21' THEN 1                   -- Rural settlement: slight uncertainty
                WHEN urban_rural_.IUR2026_V1_00 = '22' THEN 2                   -- Rural other: more ambiguous
                ELSE 0
            END),
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[pan_nz_recreational.source_data]::TEXT[],
            pan_nz_recreational.source_date,
            pan_nz_recreational.source_scale,
            ARRAY_TO_STRING(
                ARRAY_REMOVE(
                    ARRAY[
                        pan_nz_recreational.designation,
                        NULLIF(CONCAT_WS(' ', pan_nz_recreational.legislation_act, pan_nz_recreational.legislation_section), ''),
                        pan_nz_recreational.source_protection_name
                    ],
                    NULL
                ),
                E'\n'
            )
        )::nzlum_type
        WHEN dvr_public_rec_and_services.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN (
                    actual_property_use ~ '^(04|05|4(?!5)|5)'
                    AND category ~ '^O(A|H|M|R|S|X)'
                    AND outdoor_improvements IS TRUE
                    AND covenanted IS FALSE
                )
                THEN CASE WHEN category = 'OX' THEN 2 ELSE 1 END
                WHEN (
                    actual_property_use ~ '^(04|05|4(?!5)|5)'
                    AND outdoor_improvements IS TRUE
                    AND covenanted IS FALSE
                )
                THEN 2
                WHEN (
                    actual_property_use ~ '^(04|05|4(?!5)|5)'
                    AND covenanted IS FALSE
                )
                THEN 3
                WHEN (
                    actual_property_use IS NULL
                    AND outdoor_improvements IS TRUE
                    AND covenanted IS FALSE
                )
                THEN 9
                ELSE NULL
            END,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[dvr_public_rec_and_services.source_data]::TEXT[],
            dvr_public_rec_and_services.source_date,
            dvr_public_rec_and_services.source_scale,
            NULL
        )::nzlum_type
        WHEN lcdb_.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            5, -- residual urban parkland / open space
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[lcdb_.source_data]::TEXT[],
            lcdb_.source_date,
            lcdb_.source_scale,
            NULL
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM roi
    LEFT JOIN (
        SELECT h3_index,
        daterange('2011-05-22'::DATE, '2025-01-02'::DATE, '[]') AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_pond
        JOIN topo50_pond_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND topo50_pond.name = 'Lake Crichton'
    ) AS recreational_ponds ON roi.h3_index && recreational_ponds.h3_index
    LEFT JOIN (
        SELECT h3_index,
        daterange('2011-05-22'::DATE, '2025-01-03'::DATE, '[]') AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_golf_courses_h3
        WHERE :parent::h3index = h3_partition
        UNION ALL
        SELECT h3_index,
        daterange('2011-05-22'::DATE, '2024-03-20'::DATE, '[]') AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_chatham_golf_courses_h3
        WHERE :parent::h3index = h3_partition
    ) AS golf_courses ON roi.h3_index && golf_courses.h3_index
    LEFT JOIN (
        SELECT h3_index,
        daterange('2011-05-22'::DATE, '2025-01-03'::DATE, '[]') AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_sportsfields_h3
        WHERE :parent::h3index = h3_partition
    ) AS sportsfields ON roi.h3_index && sportsfields.h3_index
    LEFT JOIN (
        SELECT h3_index, source_data, source_date, source_scale, hail_category_count
        FROM hail
        WHERE hail_category_ids @> ARRAY['C2']
    ) AS hail_gun_clubs ON roi.h3_index && hail_gun_clubs.h3_index
    LEFT JOIN (
        -- PAN-NZ reserves in the same IUCN categories as class_117, but without requiring native cover.
        -- The lcdb_native join below then filters: only fire this signal where native cover is ABSENT.
        SELECT DISTINCT ON (pan_nz_draft_h3.h3_index)
        pan_nz_draft_h3.h3_index,
        source_data, source_date, source_scale,
        designation, legislation_act, legislation_section, source_protection_name
        FROM pan_nz_draft
        JOIN pan_nz_draft_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND iucn_category IN ('VI', 'Not Mapped', 'Not IUCN')
        AND designation NOT IN (
            'Ramsar List site',  -- class_117 retains these
            'World Heritage Area'
        )
        ORDER BY pan_nz_draft_h3.h3_index, source_date DESC NULLS LAST, source_id
    ) AS pan_nz_recreational ON roi.h3_index && pan_nz_recreational.h3_index
    LEFT JOIN (
        -- Non-native LCDB classes — used to exclude cells already captured by class_117
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
    LEFT JOIN (
        SELECT * FROM (
            SELECT h3_index, source_data, source_date, source_scale,
            actual_property_use,
            category,
            improvements_description ~
                '\m(SKIFIELD|SPEEDWAY|STADIUM|PAVILION|GRAND\s(STD|STAND)|SHOW\s?(GROUND|GRD)|PLAY\s?(GROUND|GRND|CTR|CENTRE)|(TENNIS|T/)\s?(COURT|CRT)|RESERVE|GOLF\s(CLUB|CRSE|COURSE)|BOWLING\s?GR(EEN)?|PARK|LODGE|(CAMP|CAMPING)|CEMETE?RY|URUPA|AMUSE(MENT)?\s?PARK|WALKWAY|(SURF|SPORTS?)\s(CLB|CLUB)|(CLUB\sROOMS?|CLBRMS?)|DOMAIN|RACECOURSE|BOTANIC|ZOO|TRAIL|CYCLE)\M'
                AS outdoor_improvements,
            legal_description ~ '\m(OPEN SPACE|QEII) COVENANT\M' AS covenanted
            FROM linz_dvr_
            WHERE (
                actual_property_use ~ '^(04|05|4(?!5)|5)'
                OR category ~ '^OS' -- Sports category
                OR actual_property_use IS NULL
            )
        ) dvr_
        WHERE dvr_.outdoor_improvements IS TRUE
    ) AS dvr_public_rec_and_services ON roi.h3_index && dvr_public_rec_and_services.h3_index
    LEFT JOIN (
        SELECT h3_index, source_data, source_date, source_scale
        FROM lcdb_
        WHERE Class_2023 = 2 -- Urban Parkland/Open Space
    ) lcdb_ ON roi.h3_index && lcdb_.h3_index
    LEFT JOIN (
        SELECT urban_rural_current_h3.h3_index, urban_rural_current.IUR2026_V1_00
        FROM urban_rural_current_h3
        JOIN urban_rural_current USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS urban_rural_ ON roi.h3_index && urban_rural_.h3_index
    WHERE recreational_ponds.h3_index IS NOT NULL
       OR golf_courses.h3_index IS NOT NULL
       OR sportsfields.h3_index IS NOT NULL
       OR hail_gun_clubs.h3_index IS NOT NULL
       OR (pan_nz_recreational.h3_index IS NOT NULL AND lcdb_native.h3_index IS NULL)
       OR dvr_public_rec_and_services.h3_index IS NOT NULL
       OR lcdb_.h3_index IS NOT NULL
);
