CREATE TEMPORARY VIEW gwrc_plantation_forests_ AS
SELECT
    h3.h3_index,
    gwrc.species_clean,
    'GWRC' AS source_data,
    DATERANGE(CURRENT_DATE, CURRENT_DATE, '[]') AS source_date,
    '[10,20]'::int4range AS source_scale,
    NULLIF(ARRAY_TO_STRING(
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN gwrc.year_planted_clean IS NOT NULL
                     THEN 'planted:' || gwrc.year_planted_clean::text END
            ] || COALESCE(
                ARRAY(SELECT 'species:' || s FROM unnest(gwrc.species_clean) s),
                ARRAY[]::text[]
            ),
            NULL
        ), E'\n'
    ), '') AS comment
FROM gwrc_plantation_forests_western gwrc
INNER JOIN gwrc_plantation_forests_western_h3 h3 USING (ogc_fid)
WHERE :parent::h3index = h3.h3_partition

UNION ALL

SELECT
    h3.h3_index,
    gwrc.species_clean,
    'GWRC' AS source_data,
    DATERANGE(CURRENT_DATE, CURRENT_DATE, '[]') AS source_date,
    '[10,20]'::int4range AS source_scale,
    NULLIF(ARRAY_TO_STRING(
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN gwrc.year_planted_clean IS NOT NULL
                     THEN 'planted:' || gwrc.year_planted_clean::text END
            ] || COALESCE(
                ARRAY(SELECT 'species:' || s FROM unnest(gwrc.species_clean) s),
                ARRAY[]::text[]
            ),
            NULL
        ), E'\n'
    ), '') AS comment
FROM gwrc_plantation_forests_eastern gwrc
INNER JOIN gwrc_plantation_forests_eastern_h3 h3 USING (ogc_fid)
WHERE :parent::h3index = h3.h3_partition;
