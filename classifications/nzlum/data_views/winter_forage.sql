CREATE TEMPORARY VIEW winter_forage_ AS (
    SELECT *
    FROM (
        SELECT
            h3_index,
            source_data,
            source_scale,
            source_date,
            manage,
            CertRating,
            CR1Case,
            -- Rank by date, so older data fills gaps in newer data but otherwise does not participate
            ROW_NUMBER() OVER (
                PARTITION BY h3_index
                ORDER BY upper(source_date) DESC
            ) AS rn
        FROM (
            -- National, 2023
            SELECT
                h3_index,
                source_data,
                source_scale,
                source_date,
                manage,
                CertRating,
                CR1Case
            FROM winter_forage_2023
            JOIN winter_forage_2023_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition

            UNION ALL

            -- National, 2022
            SELECT
                h3_index,
                source_data,
                source_scale,
                source_date,
                manage,
                CertRating,
                CR1Case
            FROM winter_forage_2022
            JOIN winter_forage_2022_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition

            UNION ALL

            -- Environment Southland, 2017
            SELECT
                h3_index,
                source_data,
                source_scale,
                source_date,
                manage,
                NULL AS CertRating,
                NULL AS CR1Case
            FROM es_winter_forage_2017
            JOIN es_winter_forage_2017_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) AS unioned
    ) AS ranked
    WHERE rn = 1
);