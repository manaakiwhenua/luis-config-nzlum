-- 3.2.3 Community services
-- Land used for providing essential services and facilities to support the local community.
-- Includes educational institutions, public healthcare facilities, libraries, museums, courts,
-- prisons, civic buildings, emergency services, marae, religious buildings, and cemeteries.
--
-- Sources:
--   topo50_cemetery: cemeteries and urupā (highest confidence — purpose-specific topo layer)
--   nz_facilities_: LINZ national facilities dataset (hospitals and schools)
--   dvr_public_rec_and_services: DVR properties with community service improvements

CREATE TEMPORARY VIEW class_323 AS (
    SELECT roi.h3_index,
    3 AS lu_code_primary,
    2 AS lu_code_secondary,
    3 AS lu_code_tertiary,
    CASE
        WHEN cemeteries.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[cemeteries.source_data]::TEXT[],
            cemeteries.source_date,
            cemeteries.source_scale,
            NULL
        )::nzlum_type
        WHEN nz_facilities_.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[nz_facilities_.source_data]::TEXT[],
            nz_facilities_.source_date,
            nz_facilities_.source_scale,
            NULL
        )::nzlum_type
        WHEN dvr_community.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN (
                    actual_property_use ~ '^(04|05|4(?!5)|5)'
                    AND category ~ '^O(A|H|M|R|X)'
                    AND community_improvements IS TRUE
                    AND covenanted IS FALSE
                )
                THEN CASE WHEN category = 'OX' THEN 2 ELSE 1 END
                WHEN (
                    actual_property_use ~ '^(04|05|4(?!5)|5)'
                    AND community_improvements IS TRUE
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
                    AND community_improvements IS TRUE
                    AND covenanted IS FALSE
                )
                THEN 9
                ELSE NULL
            END,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[dvr_community.source_data]::TEXT[],
            dvr_community.source_date,
            dvr_community.source_scale,
            NULL
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM roi
    LEFT JOIN (
        SELECT h3_index,
        daterange('2011-05-22'::DATE, '2024-12-20'::DATE, '[]') AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_cemetery_h3
        WHERE :parent::h3index = h3_partition
        UNION ALL
        SELECT h3_index,
        daterange('2011-05-22'::DATE, '2024-03-20'::DATE, '[]') AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_chatham_cemetery_h3
        WHERE :parent::h3index = h3_partition
    ) AS cemeteries ON roi.h3_index && cemeteries.h3_index
    LEFT JOIN (
        SELECT DISTINCT ON (h3_finest(nz_facilities_h3.h3_index, urban_rural_current_.h3_index))
            h3_finest(nz_facilities_h3.h3_index, urban_rural_current_.h3_index) AS h3_index,
            'LINZ' AS source_data,
            daterange(last_modified::DATE, last_modified::DATE, '[]') AS source_date,
            CASE
                WHEN urban_rural_current_.IUR2026_V1_00 IN ('11', '12', '13', '14')
                THEN '(0,1]'::int4range
                WHEN urban_rural_current_.IUR2026_V1_00 IN ('21', '22')
                THEN '[1,100]'::int4range
                ELSE 'empty'::int4range
            END AS source_scale
        FROM nz_facilities
        JOIN nz_facilities_h3 USING (ogc_fid)
        LEFT JOIN (
            SELECT urban_rural_current_h3.h3_index, urban_rural_current.IUR2026_V1_00
            FROM urban_rural_current_h3
            JOIN urban_rural_current USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) AS urban_rural_current_ ON nz_facilities_h3.h3_index && urban_rural_current_.h3_index
        WHERE :parent::h3index = h3_partition
        AND nz_facilities.use IN ('Hospital', 'School')
        AND nz_facilities.use_type NOT IN ('NGO Hospital')
        ORDER BY
            h3_finest(nz_facilities_h3.h3_index, urban_rural_current_.h3_index),
            last_modified DESC NULLS LAST
    ) nz_facilities_ ON roi.h3_index && nz_facilities_.h3_index
    LEFT JOIN (
        SELECT * FROM (
            SELECT h3_index, source_data, source_date, source_scale,
            actual_property_use,
            category,
            improvements_description ~
                '\m(OFFICE|CHURCH|WRSHP|HALL|LIBRARY|(FIRE|POLICE|AMBULANCE)\s?(STATION|STN)?|POOL|(KINDY|CHILD\s?CARE|KINDERGARTEN|PRESCH|PRESCHOOL|DAY\s?CARE|CRECHE)|SCHOOL|UNIVERSITY|MARAE|CLINIC|HOSPITAL|HOSPICE|(TOILETS|ABLUT\sBLK|ECOLOO)|RESTROOM|(COMMUN|COMMUNITY)\s(CENTRE|CENTER|CTR)|CONVENT|MONASTERY|WAR\s?(MEMRL|MEMORIAL)|(MED|MEDICAL)\s(CTR|CENTER|CENTRE|SERVICES)|MEDCNS|INFO\s(CTR|CENTER|CENTRE)|CEMETE?RY|URUPA|MONUMENT|REST\sHOME|SCOUT\sDEN|ARTS?\s(CTR|CENTRE)|SOUNDSHELL|COLLEGE|CHAPEL|CLASSRMS|KOHANGA\sREO|MUSEUM|TEMPLE|MOSQUE|PLUNKET|CHANGE\sRMS|PRISON|ED\sINST|EMBASSY|SOCIETY|THEATRE|CONSULTING\sROOMS?|AQUATIC|HISTORIC|FUNERAL\s?(HM|HOME)?)\M'
                AS community_improvements,
            legal_description ~ '\m(OPEN SPACE|QEII) COVENANT\M' AS covenanted
            FROM linz_dvr_
            WHERE (
                actual_property_use ~ '^(04|05|4(?!5)|5)'
                OR category ~ '^O(A|H|M|R|X)'
                OR actual_property_use IS NULL
            )
        ) dvr_
        WHERE dvr_.community_improvements IS TRUE
    ) AS dvr_community ON roi.h3_index && dvr_community.h3_index
    WHERE cemeteries.h3_index IS NOT NULL
       OR nz_facilities_.h3_index IS NOT NULL
       OR dvr_community.h3_index IS NOT NULL
);
