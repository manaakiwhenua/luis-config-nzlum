-- 3.2.2 Indoor recreation
-- Facilities designed for recreational activities conducted within enclosed or semi-enclosed
-- structures, including sports centres, gyms, fitness clubs, swimming pools, and indoor
-- sports arenas.
--
-- This class is identified primarily from DVR improvements descriptions indicating indoor
-- recreational infrastructure. It is a relatively thin class in the available data.
--
-- Note: Outdoor pools and open skateboard parks are classified under 3.2.1 Outdoor recreation.
-- Aquatic centres and enclosed ice rinks are captured here.

CREATE TEMPORARY VIEW class_322 AS (
    SELECT roi.h3_index,
    3 AS lu_code_primary,
    2 AS lu_code_secondary,
    2 AS lu_code_tertiary,
    CASE
        WHEN dvr_indoor_rec.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            clamp_confidence_or_null(
                CASE
                    WHEN (
                        actual_property_use ~ '^(04|05|4(?!5)|5)'
                        AND indoor_improvements IS TRUE
                        AND covenanted IS FALSE
                    )
                    THEN 2
                    WHEN (
                        actual_property_use IS NULL
                        AND indoor_improvements IS TRUE
                        AND covenanted IS FALSE
                    )
                    THEN 9
                    ELSE NULL
                END
                + CASE -- Indoor recreation facilities are primarily urban; penalise rural context
                    WHEN urban_rural_.IUR2026_V1_00 IN ('11','12','13','14') THEN 0 -- Urban: expected location
                    WHEN urban_rural_.IUR2026_V1_00 = '21' THEN 2                   -- Rural settlement: possible but uncertain
                    WHEN urban_rural_.IUR2026_V1_00 = '22' THEN 4                   -- Rural other: likely a farm building
                    ELSE 0
                END
            ),
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[dvr_indoor_rec.source_data]::TEXT[],
            dvr_indoor_rec.source_date,
            dvr_indoor_rec.source_scale,
            NULL
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM roi
    LEFT JOIN (
        SELECT * FROM (
            SELECT h3_index, source_data, source_date, source_scale,
            actual_property_use,
            improvements_description ~
                '\m(GYM|GYMNASIUM|AQUATIC|VELODROME|SKATE\s?RINK|SQUASH\s(CT|CRT|COURT)|BOWLING\s?(ALLEY|LANE)|INDOOR\s(POOL|ARENA|STADIUM|SPORTS)|SWIMMING\s(POOL|CENTRE|CENTER))\M'
                AS indoor_improvements,
            legal_description ~ '\m(OPEN SPACE|QEII) COVENANT\M' AS covenanted
            FROM linz_dvr_
            WHERE (
                actual_property_use ~ '^(04|05|4(?!5)|5)'
                OR actual_property_use IS NULL
            )
        ) dvr_
        WHERE dvr_.indoor_improvements IS TRUE
    ) AS dvr_indoor_rec ON roi.h3_index && dvr_indoor_rec.h3_index
    LEFT JOIN (
        SELECT urban_rural_current_h3.h3_index, urban_rural_current.IUR2026_V1_00
        FROM urban_rural_current_h3
        JOIN urban_rural_current USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS urban_rural_ ON roi.h3_index && urban_rural_.h3_index
    WHERE dvr_indoor_rec.h3_index IS NOT NULL
);
