-- Public recreation and services

-- This land-use type includes land designated for recreational facilities and community amenities, serving the recreational needs and essential functions of the local population.

-- Outdoor recreation

-- Land areas dedicated to leisure activities conducted in natural or semi-natural settings, such as parks, trails, beaches, sportsgrounds, camping grounds, zoos, botanic gardens, recreational reserves, sports grounds, tourist parks, mountain bike parks, etc. with a primary purpose of recreation and culture and typically with considerable unsealed vegetated areas.

-- These often cater to activities such as tramping, cycling, picnicking, and wildlife observation.

-- Parks or reserves with a high level of native bush or that are protected areas should be classified under class 1.

-- The specific identification of this land is intended to enable more ready identification of urban green space. However, this category may also be used to identify recreational areas that fall outside urban boundaries, such as mountain-bike parks.

-- Indoor recreation – facilities designed for recreational activities conducted within enclosed or semi-enclosed structures, including sports centres, gyms, fitness clubs, swimming pools, and indoor sports arenas.

-- Community services – land used for providing essential services and facilities to support the local community, including educational institutions, public healthcare facilities, libraries, museums, courts, prisons, civic buildings, emergency services, marae, religious buildings, cemeteries, and other public amenities for community functioning and well-being

CREATE TEMPORARY VIEW class_320 AS (
    SELECT h3_index,
    3 AS lu_code_primary,
    2 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN recreational_ponds.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[recreational_ponds.source_data]::TEXT[],
            recreational_ponds.source_date,
            recreational_ponds.source_scale,
            NULL
        )::nzlum_type
        WHEN (
            golf_courses.h3_index IS NOT NULL AND dvr_public_rec_and_services.h3_index IS NOT NULL -- Clipping
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[golf_courses.source_data, dvr_public_rec_and_services.source_data]::TEXT[],
            range_merge(datemultirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    golf_courses.source_date,
                    dvr_public_rec_and_services.source_date
                ], NULL
            )))::daterange,
            range_merge(int4multirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    golf_courses.source_scale,
                    dvr_public_rec_and_services.source_scale
                ], NULL
            )))::int4range,
            NULL
        )::nzlum_type
        WHEN cemeteries.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[cemeteries.source_data]::TEXT[],
            cemeteries.source_date,
            cemeteries.source_scale,
            NULL
        )::nzlum_type
        WHEN sportsfields.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[sportsfields.source_data]::TEXT[],
            sportsfields.source_date,
            sportsfields.source_scale,
            NULL
        )::nzlum_type
        WHEN nz_facilities_.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[nz_facilities_.source_data]::TEXT[], -- source_data
            nz_facilities_.source_date,
            nz_facilities_.source_scale,
            NULL
        )::nzlum_type
        WHEN hail_gun_clubs.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN hail_gun_clubs.hail_category_count = 1
                    THEN 1
                ELSE 4 -- Less confidence when there is a mixed HAIL classification
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[hail_gun_clubs.source_data]::TEXT[],
            hail_gun_clubs.source_date,
            hail_gun_clubs.source_scale,
            NULL
        )::nzlum_type
        WHEN pan_nz_draft_racecourses.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[pan_nz_draft_racecourses.source_data]::TEXT[],
            pan_nz_draft_racecourses.source_date,
            pan_nz_draft_racecourses.source_scale,
            NULL
        )::nzlum_type
        WHEN dvr_public_rec_and_services.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            -- Higher confidence for actual use evidence relative to property category evidence alone
            -- lower confidence for legal_description open space or QEII covenants (tend to apply to parts of blocks and can be obtained independently as class 1)
            CASE
                WHEN (
                    actual_property_use ~ '^(04|05|4(?!5)|5)' 
                    AND category ~ '^O(A|H|M|R|S|X)'
                    AND improvements_evidence IS TRUE
                    AND covenanted IS FALSE
                )
                THEN CASE WHEN category = 'OX' THEN 2 ELSE 1 END
                WHEN (
                    actual_property_use ~ '^(04|05|4(?!5)|5)' 
                    AND improvements_evidence IS TRUE
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
                    AND improvements_evidence IS TRUE
                    AND covenanted IS FALSE
                )
                THEN 9
                ELSE NULL
            END, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[dvr_public_rec_and_services.source_data]::TEXT[], -- source_data
            dvr_public_rec_and_services.source_date,
            dvr_public_rec_and_services.source_scale,
            NULL
        )::nzlum_type
        WHEN lcdb_.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            5, -- confidence (residual)
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[lcdb_.source_data]::TEXT[], -- source_data
            lcdb_.source_date,
            lcdb_.source_scale,
            NULL
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM (
        SELECT *,
        'LINZ' AS source_data,
        daterange(
            last_modified::DATE,
            last_modified::DATE,
            '[]'
        ) AS source_date, -- source_date
        CASE
            WHEN urban_rural_2025_.IUR2025_V1_00 IN (
                '11', -- major urban
                '12', -- large urban
                '13', -- medium urban
                '14' -- small urban
            ) -- 0.1 - 1 m in urban areas
            THEN '(0,1]'::int4range
            WHEN urban_rural_2025_.IUR2025_V1_00 IN (
                '21', -- rural settlement
                '22' -- rural other
            ) -- 1 - 100 m in rural areas
            THEN '[1,100]'::int4range
            ELSE 'empty'::int4range
        END AS source_scale-- source_scale
        FROM nz_facilities
        JOIN nz_facilities_h3 USING (ogc_fid)
        LEFT JOIN (
            SELECT
                urban_rural_2025_h3.h3_index,
                urban_rural_2025.IUR2025_V1_00
            FROM urban_rural_2025_h3
            JOIN urban_rural_2025 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) AS urban_rural_2025_ USING (h3_index)
        WHERE :parent::h3index = h3_partition
        AND nz_facilities.use IN (
            'Hospital', 'School'
        ) AND nz_facilities.use_type NOT IN (
            'NGO Hospital' -- Not public
        )
    ) nz_facilities_
    FULL OUTER JOIN (
        SELECT h3_index,
        daterange(
            '2011-05-22'::DATE,
            '2025-01-02'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_pond
        JOIN topo50_pond_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND topo50_pond.name = 'Lake Crichton' -- Recreational pond in Canterbury
    ) AS recreational_ponds USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,
        daterange(
            '2011-05-22'::DATE,
            '2025-01-03'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_golf_courses_h3
        WHERE :parent::h3index = h3_partition

        UNION ALL
        
        SELECT h3_index,
        daterange(
            '2011-05-22'::DATE,
            '2024-03-20'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_chatham_golf_courses_h3
        WHERE :parent::h3index = h3_partition
    ) AS golf_courses USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,
        daterange(
            '2011-05-22'::DATE,
            '2024-12-20'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_cemetery_h3
        WHERE :parent::h3index = h3_partition

        UNION ALL

        SELECT h3_index,
        daterange(
            '2011-05-22'::DATE,
            '2024-03-20'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_chatham_cemetery_h3
        WHERE :parent::h3index = h3_partition
    ) AS cemeteries USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,
        daterange(
            '2011-05-22'::DATE,
            '2025-01-03'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_sportsfields_h3
        WHERE :parent::h3index = h3_partition
    ) AS sportsfields USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,
        source_data,
        source_date,
        source_scale,
        actual_property_use,
        category,
        CASE
            WHEN improvements_description ~
                '\m(SKIFIELD|RACECOURSE|DOMAIN|SPEEDWAY|STADIUM|PAVILION|GRAND\s(STD|STAND)|SHOW\s?(GROUND|GRD)|SKATE\s?RINK|PLAY\s?(GROUND|GRND|CTR|CENTRE)|(TENNIS|T/)\s?(COURT|CRT)|(FIRE|POLICE|AMBULANCE)\s?(STATION|STN)?|OFFICE|CHURCH|WRSHP|HALL|LIBRARY|POOL|(KINDY|CHILD\s?CARE|KINDERGARTEN|PRESCH|PRESCHOOL|DAY\s?CARE|CRECHE)|SCHOOL|UNIVERSITY|MARAE|RESERVE|GOLF\s(CLUB|CRSE|COURSE)|(CLUB\sROOMS?|CLBRMS?)|PARK|LODGE|CLINIC|HOSPITAL|HOSPICE|(TOILETS|ABLUT\sBLK|ECOLOO)|RESTROOM|WALKWAY|(COMMUN|COMMUNITY)\s(CENTRE|CENTER|CTR)|SQUASH\s(CT|CRT|COURT)|(SURF|SPORTS?)\s(CLB|CLUB)|CONVENT|MONASTERY|WAR\s?(MEMRL|MEMORIAL)|(MED|MEDICAL)\s(CTR|CENTER|CENTRE|SERVICES)|MEDCNS|INFO\s(CTR|CENTER|CENTRE)|(CAMP|CAMPING)|CEMETE?RY|URUPA|BOWLING|AMUSE(MENT)?\s?PARK|(GYM|GYMNASIUM)|MONUMENT|REST\sHOME|SCOUT\sDEN|ARTS?\s(CTR|CENTRE)|SOUNDSHELL|COLLEGE|CHAPEL|CLASSRMS|BOWLING\s?GR(EEN)?|KOHANGA\sREO|MUSEUM|TEMPLE|THEOSPOPHICAL|MOSQUE|PLUNKET|CHANGE\sRMS|PRISON|VELODROME|ED\sINST|EMBASSY|SOCIETY|THEATRE|CONSULTING\sROOMS?|PERGOLA|AQUATIC|HISTORIC|FUNERAL\s?(HM|HOME)?)\M'
            THEN TRUE
            ELSE FALSE
        END AS improvements_evidence,
        CASE
            WHEN legal_description ~ '\m(OPEN SPACE|QEII) COVENANT\M'
            THEN TRUE
            ELSE FALSE
        END AS covenanted
        FROM linz_dvr_
        WHERE (
            actual_property_use ~ '^(04|05|4(?!5)|5)'
            OR category ~ '^O(A|H|M|R|S|X)' -- Other (assembly halls, health, Maori, religious, sports, other/multiple)
            OR actual_property_use IS NULL
        )
    ) AS dvr_public_rec_and_services USING (h3_index)
    FULL OUTER JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale
        FROM lcdb_
        WHERE Class_2018 = 2 -- Urban Parkland/Open Space
    ) lcdb_ USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,
        daterange(
            '2011-05-22'::DATE,
            '2025-01-02'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_pond
        JOIN topo50_pond_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND pond_use = 'ice skating'
    ) AS ice_skating USING (h3_index)
    FULL OUTER JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale,
            hail_category_count
        FROM hail
        WHERE hail_category_ids @> ARRAY[
            'C2' -- Gun clubs or rifle ranges, including clay targets clubs that use lead munitions outdoors
        ]
    ) AS hail_gun_clubs USING (h3_index)
    FULL OUTER JOIN (
        SELECT DISTINCT ON (h3_index)
        h3_index,
        source_data,
        source_date,
        source_scale
        FROM pan_nz_draft
        JOIN pan_nz_draft_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND legislation_act = 'RESERVES_ACT'
        AND legislation_section = 'S16_11_RECREATION_RESERVE_RACECOURSE'
        ORDER BY
            h3_index,
            source_date DESC NULLS LAST, -- Prefer more recent
            source_id -- Tie-break
    ) AS pan_nz_draft_racecourses USING (h3_index)
);

-- use DVR
    -- improvement "FIRE STN" "HOSPITAL", etc.

-- TODO use CROSL?