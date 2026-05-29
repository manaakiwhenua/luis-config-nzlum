-- This land-use type includes land allocated to providing electricity, gas, or water.

--     Fuel powered electricity generation – includes facilities that produce electricity by burning fossil fuels such as coal, oil, and natural gas.

--     Hydroelectricity generation – facilities that use the energy of flowing or falling water, typically through hydroelectric dams, converting hydraulic energy into electrical power. Includes dams and canals.

--     Wind electricity generation – power generation from wind, including wind farms.

--     Solar electricity generation – facilities that harness sunlight using photovoltaic cells or solar thermal systems to convert solar radiation into electrical power.

--     Electricity substations and transmission – facilities and infrastructure associated with the distribution and transmission of electrical power from generation sources to end- users, including substations, transformers, and large, high-voltage transmission towers (pylons).

--     Gas treatment, storage, and transmission – facilities and infrastructure involved in the processing, storage, and transportation of natural gas, including gas treatment plants, storage facilities, and pipelines for transmission to consumers.

--     Water extraction and transmission – facilities and infrastructure for extracting, purifying, treating, and transporting water from natural sources such as rivers, lakes, or reservoirs to meet various human needs, including drinking-water supply, irrigation, and industrial use. Includes drinking-water reservoirs themselves

CREATE TEMPORARY VIEW class_350 AS (
    SELECT roi.h3_index,
    3 AS lu_code_primary,
    5 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN (
            topo50_beacons.h3_index IS NOT NULL
            OR topo50_masts.h3_index IS NOT NULL
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[topo50_beacons.source_data, topo50_masts.source_data]::TEXT[],
            range_merge(datemultirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    topo50_beacons.source_date,
                    topo50_masts.source_date
                ], NULL)
            ))::daterange,
            range_merge(int4multirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    topo50_beacons.source_scale,
                    topo50_masts.source_scale
                ], NULL)
            ))::int4range,
            NULL
        )::nzlum_type
        WHEN topo50_hydro_and_reservoirs.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[topo50_hydro_and_reservoirs.source_data]::TEXT[],
            topo50_hydro_and_reservoirs.source_date,
            topo50_hydro_and_reservoirs.source_scale,
            NULL
        )::nzlum_type
        WHEN (
            linz_dvr_utility_.h3_index IS NOT NULL
            AND linz_crosl_.h3_index IS NOT NULL
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN lcdb_unbuilt.h3_index IS NOT NULL
                THEN 6 -- LCDB not in a settled area
                ELSE 1
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[linz_dvr_utility_.source_data, linz_crosl_.source_data]::TEXT[],
            range_merge(
                linz_dvr_utility_.source_date,
                linz_crosl_.source_date
            )::daterange,
            range_merge(
                linz_dvr_utility_.source_scale,
                linz_crosl_.source_scale
            )::int4range,
            NULL
        )::nzlum_type
        WHEN linz_crosl_.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN lcdb_unbuilt.h3_index IS NOT NULL
                THEN 7 -- LCDB not in a settled area
                ELSE 1
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[linz_crosl_.source_data]::TEXT[],
            linz_crosl_.source_date,
            linz_crosl_.source_scale,
            NULL
        )::nzlum_type
        WHEN (
            linz_dvr_utility_.actual_property_use ~ '6[0123467]'
            OR linz_dvr_utility_.actual_property_use = '06'
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN lcdb_unbuilt.h3_index IS NOT NULL
                THEN -- LCDB not in a settled area
                    CASE
                        WHEN linz_dvr_utility_.improvements_evidence
                        THEN 7
                        ELSE 8
                    END
                ELSE
                    CASE
                        WHEN linz_dvr_utility_.improvements_evidence
                        THEN 1
                        ELSE 2
                    END
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[linz_dvr_utility_.source_data]::TEXT[],
            linz_dvr_utility_.source_date,
            linz_dvr_utility_.source_scale,
            NULL
        )::nzlum_type
        WHEN hail_electric.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN hail_electric.hail_category_count = 1
                    THEN 1
                ELSE 4 -- Less confidence when there is a mixed HAIL classification
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[hail_electric.source_data]::TEXT[],
            hail_electric.source_date,
            hail_electric.source_scale,
            NULL
        )::nzlum_type
    END AS nzlum_type
    FROM roi
    LEFT JOIN (
        SELECT h3_index,
            'LINZ' AS source_data,
            DATERANGE(CURRENT_DATE, CURRENT_DATE, '[]') AS source_date,
            '[50,100]'::int4range AS source_scale
        FROM topo50_beacons_h3
        WHERE :parent::h3index = h3_partition
        UNION ALL
        SELECT h3_index,
            'LINZ' AS source_data,
            DATERANGE(CURRENT_DATE, CURRENT_DATE, '[]') AS source_date,
            '[50,100]'::int4range AS source_scale
        FROM topo50_chatham_beacons_h3
        WHERE :parent::h3index = h3_partition
    ) AS topo50_beacons ON roi.h3_index && topo50_beacons.h3_index
    LEFT JOIN (
        SELECT h3_index,
            'LINZ' AS source_data,
            DATERANGE(CURRENT_DATE, CURRENT_DATE, '[]') AS source_date,
            '[50,100]'::int4range AS source_scale
        FROM topo50_masts_h3
        WHERE :parent::h3index = h3_partition
        UNION ALL
        SELECT h3_index,
            'LINZ' AS source_data,
            DATERANGE(CURRENT_DATE, CURRENT_DATE, '[]') AS source_date,
            '[50,100]'::int4range AS source_scale
        FROM topo50_chatham_masts_h3
        WHERE :parent::h3index = h3_partition
    ) AS topo50_masts ON roi.h3_index && topo50_masts.h3_index
    LEFT JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale
        FROM linz_crosl_
        WHERE managed_by IN (
            'Transpower New Zealand Limited',
            'Genesis Energy Limited',
            'Meridian Energy Limited',
            'Mighty River Power Limited - Grantee',
            'Watercare Services Limited'
        ) OR statutory_actions ~* '\m(substation|electricity|water\s?power|pump\s?station|water\s?works)\M'
    ) AS linz_crosl_ ON roi.h3_index && linz_crosl_.h3_index
    LEFT JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale,
            actual_property_use,
            CASE
                WHEN improvements_description ~ '\m(RESERVOIRS?)\M'
                THEN TRUE
                ELSE FALSE
            END AS improvements_evidence
        FROM linz_dvr_
        WHERE
            actual_property_use ~ '6[0123467]' -- 65 is sanitary (better as 3.8.X) and 69 is vacant (3.9.X)
            OR actual_property_use = '06'
            OR category ~ '^U[CEGT]' -- UP (postboxes) and UR (rail network corridors) excluded
    ) AS linz_dvr_utility_ ON roi.h3_index && linz_dvr_utility_.h3_index
    LEFT JOIN (
        SELECT
            h3_index,
            'LINZ' AS source_data,
            daterange(
                '2011-05-22'::DATE,
                '2025-01-02'::DATE,
                '[]'
            ) AS source_date,
            '[1,100]'::int4range AS source_scale
        FROM topo50_lake
        JOIN topo50_lake_h3 USING (ogc_fid)
        WHERE
            :parent::h3index = h3_partition
            AND lake_use IN (
                'hydro-electric',
                'reservoir'
            ) OR "name" IN ( -- Hydro-electric dams that are not identified as such in the LINZ Topo50 lakes dataset
                'Lake Moawhango',
                'Lake Rotoaira',
                'Lake Otamangakau',
                'Lake Te Whaiau',
                'Lake Whakamaru',
                'Lake Maraetai',
                'Lake Arapuni'
            )
    ) AS topo50_hydro_and_reservoirs ON roi.h3_index && topo50_hydro_and_reservoirs.h3_index
    LEFT JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale,
            hail_category_count
        FROM hail
        WHERE hail_category_ids @> ARRAY[
            'B4' -- Power stations, substations or switchyards
        ]
    ) AS hail_electric ON roi.h3_index && hail_electric.h3_index
    LEFT JOIN (
        SELECT h3_index
        FROM lcdb_
        WHERE Class_2018 NOT IN (
            1, -- Settlement
            2, -- Urban parkland open space
            5 -- Transport infrastructure
        )
    ) AS lcdb_unbuilt ON roi.h3_index && lcdb_unbuilt.h3_index
    WHERE topo50_beacons.h3_index IS NOT NULL
       OR topo50_masts.h3_index IS NOT NULL
       OR linz_crosl_.h3_index IS NOT NULL
       OR linz_dvr_utility_.h3_index IS NOT NULL
       OR topo50_hydro_and_reservoirs.h3_index IS NOT NULL
       OR hail_electric.h3_index IS NOT NULL
);
-- DVR use class 6 (all, except 69? 67 (postboxes) is odd and may fit under comm. services)
-- TODO what to do with actual use 67 postboxes
-- category UR Rail nework corridors are better under transport

-- CROSL 'electricity substation' etc
-- CROSL managed by
    -- Transpower New Zealand Limited
    -- Genesis Energy Limited
    -- Meridian Energy Limited
    -- Mighty River Power Limited - Grantee
    -- etc.

-- LINZ lakes purpose "hydro-electric" and "reservoir"
-- LINZ lakes "Lake Moawhango" (hydro electric), Lake Rotoaira, Arapuni, Lake Otamangakau, Lake Te Whaiau, Lake Whakamaru, Lake Maraetai, Lake Arapuni

-- HAIL for electrical substations etc. that may otherwise be absent