CREATE TEMPORARY VIEW class_360 AS ( --Transport and communicaton
    SELECT DISTINCT ON (h3_index) h3_index,
    3 AS lu_code_primary,
    6 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN topo_airport.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[topo_airport.source_data]::TEXT[],
            topo_airport.source_date,
            topo_airport.source_scale
        )::nzlum_type
        WHEN topo50_runways.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN topo50_runways.status = 'disused'
                THEN 4
                ELSE 1 
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[topo50_runways.source_data]::TEXT[],
            topo50_runways.source_date,
            topo50_runways.source_scale
        )::nzlum_type
        WHEN (
            topo_rail.h3_index IS NOT NULL
            OR topo_road.h3_index IS NOT NULL
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            2,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[topo_rail.source_data, topo_road.source_data]::TEXT[],
            range_merge(datemultirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    topo_rail.source_date,
                    topo_road.source_date
                ], NULL
            )))::daterange,
            range_merge(int4multirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    topo_rail.source_scale,
                    topo_road.source_scale
                ], NULL
            )))::int4range
        )::nzlum_type
        WHEN hail_transport.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN hail_category_count = 1
                    THEN 2
                ELSE 4 -- Less confidence when there is a mixed HAIL classification
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[hail_transport.source_data]::TEXT[],
            hail_transport.source_date,
            hail_transport.source_scale
        )::nzlum_type
        WHEN (
            parcel_rail.h3_index IS NOT NULL
            OR parcel_road.h3_index IS NOT NULL
        )
        THEN ROW(
            ARRAY[]::TEXT[],
            CASE
                WHEN urban_rural_2025_.IUR2025_V1_00 = '22'
                THEN 8 -- Often paper roads in rural areas
                ELSE 2 -- Urban areas have better defined road parcels
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[lcdb_.source_data, parcel_rail.source_data, parcel_road.source_data]::TEXT[],
            range_merge(datemultirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    parcel_rail.source_date,
                    parcel_road.source_date,
                    lcdb_.source_date
                ], NULL)
            ))::daterange,
            range_merge(int4multirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    parcel_rail.source_scale,
                    parcel_road.source_scale,
                    lcdb_.source_scale
                ], NULL)
            ))::int4range
        )::nzlum_type
        WHEN dvr_transport.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN dvr_transport.actual_property_use NOT IN ('03', '30')
                THEN 7
                ELSE 11
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[dvr_transport.source_data]::TEXT[],
            dvr_transport.source_date,
            dvr_transport.source_scale
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM (
        SELECT h3_index,
        'LINZ' AS source_data,
        DATERANGE(
            '2012-12-20'::DATE,
            '2025-03-15'::DATE,
            '[]'
            ) AS source_date,
        '[1,100]'::int4range AS source_scale
        FROM railway_parcels
        JOIN railway_parcels_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS parcel_rail
    FULL OUTER JOIN (
        SELECT h3_index,
        'LINZ' AS source_data,
        DATERANGE(
            '2011-05-22'::DATE,
            '2025-01-03'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale
        FROM topo50_railway
        JOIN topo50_railway_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS topo_rail USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,
        'LINZ' AS source_data,
        DATERANGE(
            '2012-12-20'::DATE,
            '2025-03-15'::DATE,
            '[]'
        ) AS source_date,
        -- TODO is this the appropriate scale? Does it vary between urban and rural areas?
        '[1,100]'::int4range AS source_scale
        FROM road_parcels
        JOIN road_parcels_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS parcel_road USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,
        'LINZ' AS source_data,
        DATERANGE(
            '2011-05-22'::DATE,
            '2025-01-03'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale
        FROM topo50_roads_h3
        WHERE :parent::h3index = h3_partition

        UNION ALL

        SELECT h3_index,
        'LINZ' AS source_data,
        DATERANGE(
            '2011-05-22'::DATE,
            '2024-03-20'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale
        FROM topo50_chatham_roads_h3
        WHERE :parent::h3index = h3_partition
    ) AS topo_road USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,
        'LINZ' AS source_data,
        DATERANGE(
            '2011-05-22'::DATE,
            '2024-12-18'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale
        FROM topo50_airports_h3
        WHERE :parent::h3index = h3_partition
        
        UNION ALL

        SELECT h3_index,
        'LINZ' AS source_data,
        DATERANGE(
            '2011-05-22'::DATE,
            '2024-05-20'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale
        FROM topo50_chatham_airports_h3
        WHERE :parent::h3index = h3_partition
    ) AS topo_airport USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,
        "status",
        'LINZ' AS source_data,
        DATERANGE(
            '2011-05-22'::DATE,
            '2024-04-20'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale
        FROM topo50_runways
        JOIN topo50_runways_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition

        UNION ALL

        SELECT h3_index,
        "status",
        'LINZ' AS source_data,
        DATERANGE(
            '2011-05-22'::DATE,
            '2024-04-20'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale
        FROM topo50_chatham_runways
        JOIN topo50_chatham_runways_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS topo50_runways USING (h3_index)
    FULL OUTER JOIN (
        SELECT *
        FROM linz_dvr_
        WHERE actual_property_use LIKE '3%' -- Transport
        OR actual_property_use = '03' -- Multi-use within transport
    ) AS dvr_transport USING (h3_index)
    FULL OUTER JOIN (
        SELECT *
        FROM hail
        WHERE hail_category_ids @> ARRAY[
            'F1', -- Airports including fuel storage, workshops, washdown areas, or fire practice areas 
            'F5', -- Port activities including dry docks or marine vessel maintenance facilities
            'F6' -- Railway yards including goods-handling yards, workshops, refuelling facilities or maintenance areas
        ]
    ) AS hail_transport USING (h3_index)
    LEFT JOIN lcdb_ USING (h3_index)
    LEFT JOIN (
        SELECT
            urban_rural_2025_h3.h3_index,
            urban_rural_2025.IUR2025_V1_00
        FROM urban_rural_2025_h3
        JOIN urban_rural_2025 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS urban_rural_2025_ USING (h3_index)
);

-- TODO CROSL Ports of Auckland, Airways, etc.