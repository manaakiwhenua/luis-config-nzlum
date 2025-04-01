CREATE TEMPORARY VIEW lum_ AS (
    SELECT
        h3_index,
        lucid_2020,
        subid_2020,
        daterange(
            TO_DATE(map_year::text, 'YYYY'),
            TO_DATE((map_year + 1)::TEXT, 'YYYY'),
            '[)'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale, -- Assume same as LCDB v5
        'LUCAS LUM' AS source_data,
        '2' AS confidence
    FROM (
        SELECT h3_index, lucid_2020, subid_2020, map_year
        FROM lum_h3
        INNER JOIN lum USING (ogc_fid)
        WHERE :parent::h3index = h3_partition

        UNION ALL

        SELECT h3_index, lucid_2020, subid_2020, map_year
        FROM lum_chathams_h3
        INNER JOIN lum_chathams USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS combined_lum
);