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
            CASE
                WHEN (
                    pan_nz_draft_h3.h3_index IS NULL
                    AND rural.h3_index IS NOT NULL
                )
                THEN 2
                ELSE 12
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[lum_.source_data]::TEXT[], -- source_data
            lum_.source_date,
            lum_.source_scale
        )::nzlum_type
        WHEN (
            consents_forestry.h3_index IS NOT NULL
            AND (
                topo50_exotic_polygons_.h3_index IS NOT NULL
                OR lum_.h3_index IS NOT NULL
            )
        )
        -- TODO lower confidence with hydro parcels?
        THEN ROW (
            ARRAY[]::TEXT[],
            1,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[topo50_exotic_polygons_.source_data, consents_forestry.source_data]::TEXT[], -- source_data
            range_merge(datemultirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    topo50_exotic_polygons_.source_date,
                    consents_forestry.source_date,
                    lum_.source_date
                ], NULL)
            ))::daterange, -- source_date
            range_merge(int4multirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    topo50_exotic_polygons_.source_scale,
                    consents_forestry.source_scale,
                    lum_.source_scale
                ], NULL)
            ))::int4range -- source_date
        )::nzlum_type
        WHEN (
            consents_forestry.h3_index IS NULL
            AND (
                topo50_exotic_polygons_.h3_index IS NOT NULL
                OR lum_.h3_index IS NOT NULL
            )
        )
        THEN ROW (
            ARRAY[]::TEXT[],
            CASE
                WHEN (
                    pan_nz_draft_h3.h3_index IS NULL
                    AND rural.h3_index IS NOT NULL
                ) THEN 2
                ELSE 12
            END,
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
            ))::int4range
        )::nzlum_type
        WHEN consents_forestry.afforestation_flag IS TRUE
        THEN ROW ( -- Very recent forests?
            ARRAY[]::TEXT[],
            4,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[consents_forestry.source_data]::TEXT[],
            consents_forestry.source_date,
            consents_forestry.source_scale
        )::nzlum_type
    END AS nzlum_type
    -- commodity type? pinus radiata, douglas fir (lum_.subid_2020)
    -- species (topo50: empty=coniferous, and "non-coniferous")
    FROM (
        SELECT *
        FROM lum_
        WHERE lum_.lucid_2020 IN (
            '72 - Planted Forest - Pre 1990',
            '73 - Post 1989 Forest'
        )
    ) AS lum_
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
    FULL OUTER JOIN consents_forestry USING (h3_index)
    LEFT JOIN pan_nz_draft_h3 USING (h3_index)
    LEFT JOIN (
        SELECT *
        FROM urban_rural_2025
        JOIN urban_rural_2025_h3 USING (ogc_fid)
        WHERE urban_rural_2025.IUR2025_V1_00 IN (
            '22', -- Rural other
            '31', -- Inland water
            '32', -- Inlet
            '33' -- Oceanic
        )
    ) AS rural USING (h3_index)
);
-- TODO use CROSL
-- TODO use DVR