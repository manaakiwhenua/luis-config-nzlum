CREATE TEMPORARY VIEW lcdb_ AS (
    SELECT
        h3_index,
        h3_partition,
        -- Name_2018 AS comment,
        Class_2018::int,
        daterange(
            EditDate::DATE,
            EditDate::DATE,
            '[]'
         ) AS source_date,
        '[60,100)'::int4range AS source_scale, -- 10 m pansharpened image pixels accurate to within 5 m 95% RMSE, digitised at 1:50000 (1/1000*50000) scale; minimum width of 50 m for change detection
        'LCDB v5' AS source_data
    FROM (
        SELECT
            h3_index,
            h3_partition,
            Class_2018::int,
            EditDate
        FROM lcdb_v5_h3
        INNER JOIN lcdb_v5 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    
        UNION ALL
        
        -- NB LCDB Chatham Islands adds two classes
        -- 80 'Peat Shrubland (Chatham Is)'
        -- 81 'Dune Shrubland (Chatham Is)'
        -- Otherwise it is the same
        SELECT
            h3_index,
            h3_partition,
            Class_2018::int,
            EditDate
        FROM lcdb_v5_chathams_h3
        INNER JOIN lcdb_v5_chathams USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS combined_lcdb
);