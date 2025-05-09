BEGIN;

LOCK TABLE :classification IN ACCESS EXCLUSIVE MODE;

ALTER TABLE :classification DETACH PARTITION :partition;
COMMIT;

TRUNCATE TABLE :partition;

CREATE UNLOGGED TABLE :partition (
    LIKE :classification
    INCLUDING DEFAULTS INCLUDING CONSTRAINTS
);

\set ON_ERROR_STOP on

CREATE TEMPORARY VIEW roi AS (
    SELECT h3_index, h3_partition FROM :roi
    WHERE :parent::h3index = h3_partition
);

\ir data_views/lcdb.sql
\ir data_views/linz_crosl.sql
\ir data_views/linz_dvr.sql
\ir data_views/lum.sql
\ir data_views/irrigation.sql
\ir data_views/marine_farms.sql
\ir data_views/crop_mapping.sql
\ir data_views/hail.sql
\ir data_views/winter_forage.sql
\ir data_views/transitional_land.sql
\ir data_views/consents/dairy_effluent_discharge.sql
\ir data_views/consents/forestry.sql
\ir data_views/consents/pastoral_farms.sql

\ir classes/class_111.sql
\ir classes/class_112.sql
\ir classes/class_113.sql
\ir classes/class_114.sql
\ir classes/class_115.sql
\ir classes/class_116.sql
\ir classes/class_117.sql
\ir classes/class_120.sql
\ir classes/class_130.sql
\ir classes/class_136.sql
\ir classes/class_140.sql

\ir classes/class_210.sql
\ir classes/class_220.sql
\ir classes/class_230.sql
\ir classes/class_240.sql
\ir classes/class_250.sql
\ir classes/class_260.sql
\ir classes/class_270.sql
-- \ir classes/class_280.sql
\ir classes/class_280_v2.sql

\ir classes/class_310.sql
\ir classes/class_320.sql
\ir classes/class_330.sql
\ir classes/class_340.sql
\ir classes/class_350.sql
\ir classes/class_360.sql
\ir classes/class_370.sql
\ir classes/class_380.sql
\ir classes/class_390.sql

-- water
\ir attributes/water.sql
-- land_status
\ir attributes/land_status.sql
 -- zone, land_estate
\ir attributes/zone.sql

-- TODO is the compact/uncompact cycle necessary for this classification?
INSERT INTO :partition
SELECT
    h3_cell_to_parent(h3_index_compact, :resolution) AS h3_parent,
    *
FROM (
    SELECT
        h3_compact_cells(ARRAY_AGG(DISTINCT h3_index)) AS h3_index_compact,
        hash_bigint(ARRAY[
            lu_code_primary,
            lu_code_secondary,
            lu_code_tertiary,
            array_to_string(lu_code_ancillary, ', '),
            confidence,
            array_to_string(commod, ', '),
            array_to_string(manage, ', '),
            source_scale::TEXT, -- Convert from a INT4RANGE to TEXT
            source_date::TEXT, -- Convert from DATERANGE to TEXT
            array_to_string(source_data, ', ')
        ]::TEXT[]) AS unit_id,
        lu_code_primary::INT,
        lu_code_secondary::INT,
        lu_code_tertiary::INT,
        ARRAY_TO_STRING(ARRAY[lu_code_primary, lu_code_secondary, lu_code_tertiary]::TEXT[], '.') AS lu_code,
        nzsluc_v0_2_0_lu_description(lu_code_primary, lu_code_secondary, lu_code_tertiary) AS lu_description,
        lu_code_ancillary,
        commod,
        manage,
        land_zone.land_estate,
        land_status.land_status,
        water_features.feature AS water,
        land_zone.zone,
        --CEIL(confidence/3.0) AS confidence, -- Collapse from range 1-12 to 1-4, after internal sorting in full range
        confidence,
        CURRENT_DATE AS luc_date,
        source_data,
        source_date, -- type DATERANGE
        source_scale, -- type INT4RANGE
        comment
    FROM (
        SELECT
            roi.h3_index,
            lu_code_primary,
            lu_code_secondary,
            lu_code_tertiary,
            NULLIF(
                deduplicate_and_sort((nzlum_type).lu_code_ancillary),
                ARRAY[]::TEXT[]
            ) AS lu_code_ancillary,
            (nzlum_type).confidence AS confidence,
            NULLIF(
                deduplicate_and_sort((nzlum_type).commod),
                ARRAY[]::TEXT[]
            ) AS commod,
            NULLIF(
                deduplicate_and_sort((nzlum_type).manage),
                ARRAY[]::TEXT[]
            ) AS manage,
            NULLIF(
                deduplicate_and_sort((nzlum_type).source_data),
                ARRAY[]::TEXT[]
            ) AS source_data,
            (nzlum_type).source_date AS source_date,
            -- TODO many layers without a date field could still have a recorded publication date, e.g. using kart or Koordinates API; use of CURRENT_DATE is current behaviour and will cause incongruity if a large query processes overnight
            (nzlum_type).source_scale AS source_scale, -- NB use range_union to combine ranges from input if appropriate
            (nzlum_type).comment AS comment,
            ROW_NUMBER() OVER (
                PARTITION BY roi.h3_index
                ORDER BY
                    (nzlum_type).confidence ASC NULLS LAST, -- Prefer more confident
                    CASE
                        WHEN (
                            lu_code_primary = 1
                            AND lu_code_secondary = 1
                        )
                        THEN lu_code_tertiary
                        ELSE NULL
                    END ASC NULLS LAST, -- Overlapping protected areas with equal confidence; prefer more protected, ignoring dates and scales. All others consider equally and defer to other sort criteria
                    upper((nzlum_type).source_date) DESC NULLS FIRST,  -- Sort by upper bound, with nulls (unbounded) first, then latest stated
                    lower((nzlum_type).source_date) ASC NULLS LAST, -- Sort by lower bound, with nulls (unbounded) first, then earliest stated
                    (nzlum_type).source_scale ASC NULLS LAST, -- NB rules for int4range sorting, open ends are infinite
                    CAST(
                        CASE
                            WHEN lu_code_primary IS NOT NULL
                            THEN ARRAY_TO_STRING(ARRAY[lu_code_primary, lu_code_secondary, lu_code_tertiary]::TEXT[], '.')
                            ELSE '0'
                        END
                    AS nzlum0_lu_coden) ASC NULLS LAST -- Sort on enum (preferential order as a tie-break)
            ) AS rn
        FROM (
            SELECT * FROM class_111 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_112 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_113 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_114 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_115 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_116 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_117 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_120 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_130 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_136 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_140 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_210 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_220 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_230 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_240 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_250 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_260 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_270 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_280 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_310 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_320 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_330 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_340 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_350 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_360 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_370 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_380 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
            UNION ALL
            SELECT * FROM class_390 WHERE lu_code_primary IS NOT NULL AND (nzlum_type).confidence IS NOT NULL
        ) AS nzlum_union
        RIGHT JOIN roi USING (h3_index)
        WHERE :parent::h3index = roi.h3_partition
    ) subq
    FULL OUTER JOIN land_zone USING (h3_index)
    FULL OUTER JOIN water_features USING (h3_index)
    FULL OUTER JOIN land_status USING (h3_index)
    -- TODO consider a function for recording multiple uses
    WHERE rn = 1 -- Select best option after ranking in cases where multiple classes are possible
    GROUP BY
        unit_id,
        lu_code_primary,
        lu_code_secondary,
        lu_code_tertiary,
        lu_code,
        lu_code_ancillary,
        commod,
        manage,
        land_estate,
        land_status,
        water,
        zone,
        confidence,
        luc_date,
        source_data,
        source_date,
        source_scale,
        comment
    ORDER BY h3_index_compact DESC
) q;

\unset ON_ERROR_STOP

BEGIN;
LOCK TABLE :classification IN ACCESS EXCLUSIVE MODE;
ALTER TABLE :classification ATTACH PARTITION :partition FOR VALUES IN (:parent::h3index);

COMMIT;