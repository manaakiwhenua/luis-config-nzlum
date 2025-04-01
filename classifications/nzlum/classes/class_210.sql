CREATE TEMPORARY VIEW class_210 AS ( -- Plantation forests
    SELECT h3_index,
    2 AS lu_code_primary,
    1 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE 
        WHEN (
            lum_.subid_2020 = '122 - Wilding trees'
            AND topo50_exotic_polygons_.h3_index IS NOT NULL
        )
        THEN ROW(
            ARRAY[]::TEXT[],
            5,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[topo50_exotic_polygons_.source_data, lum_.source_data]::TEXT[], -- source_data
            range_merge(datemultirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    topo50_exotic_polygons_.source_date,
                    lum_.source_date
                ], NULL)
            ))::daterange, -- source_date
            range_merge(int4multirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    topo50_exotic_polygons_.source_scale,
                    lum_.source_scale
                ], NULL)
            ))::int4range -- source_scale
        )::nzlum_type
        WHEN lum_.subid_2020 = '122 - Wilding trees'
        THEN ROW(
            ARRAY[]::TEXT[],
            5,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[lum_.source_data]::TEXT[], -- source_data
            lum_.source_date,
            lum_.source_scale
        )::nzlum_type
        WHEN topo50_exotic_polygons_.h3_index IS NOT NULL
        -- TODO lower confidence with hydro parcels 
        THEN ROW (
            ARRAY[]::TEXT[],
            2,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[topo50_exotic_polygons_.source_data]::TEXT[], -- source_data
            topo50_exotic_polygons_.source_date,
            topo50_exotic_polygons_.source_scale
        )::nzlum_type
        -- TODO lower confidence with hydro parcels 
        ELSE ROW (
            ARRAY[]::TEXT[],
            2,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[lum_.source_data]::TEXT[], -- source_data
            lum_.source_date, -- source_date
            lum_.source_scale -- source_scale
        )::nzlum_type
    END AS nzlum_type
    -- commodity type? pinus radiata, douglas fir (lum_.subid_2020)
    -- species (topo50: empty=coniferous, and "non-coniferous")
    FROM lum_
    FULL OUTER JOIN (
        SELECT *,
        DATERANGE(
            '2011-05-22',
            '2025-03-07',
            '[]'
        ) AS source_date,
        'LINZ' AS source_data,
        '[60,100)'::int4range AS source_scale
        FROM topo50_exotic_polygons_h3
        WHERE :parent::h3index = h3_partition
        UNION ALL
        SELECT *,
        DATERANGE(
            '2011-05-22',
            '2024-03-20',
            '[]'
        ) AS source_date,
        'LINZ' AS source_data,
        '[60,100)'::int4range AS source_scale
        FROM topo50_chatham_exotic_polygons_h3
        WHERE :parent::h3index = h3_partition
    ) topo50_exotic_polygons_ USING (h3_index)
    WHERE lum_.lucid_2020 IN (
        '72 - Planted Forest - Pre 1990',
        '73 - Post 1989 Forest'
    )
);
-- TODO use CROSL
-- TODO use DVR