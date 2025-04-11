-- This land-use type includes land allocated to providing electricity, gas, or water.

--     Fuel powered electricity generation – includes facilities that produce electricity by burning fossil fuels such as coal, oil, and natural gas.

--     Hydroelectricity generation – facilities that use the energy of flowing or falling water, typically through hydroelectric dams, converting hydraulic energy into electrical power. Includes dams and canals.

--     Wind electricity generation – power generation from wind, including wind farms.

--     Solar electricity generation – facilities that harness sunlight using photovoltaic cells or solar thermal systems to convert solar radiation into electrical power.

--     Electricity substations and transmission – facilities and infrastructure associated with the distribution and transmission of electrical power from generation sources to end- users, including substations, transformers, and large, high-voltage transmission towers (pylons).

--     Gas treatment, storage, and transmission – facilities and infrastructure involved in the processing, storage, and transportation of natural gas, including gas treatment plants, storage facilities, and pipelines for transmission to consumers.

--     Water extraction and transmission – facilities and infrastructure for extracting, purifying, treating, and transporting water from natural sources such as rivers, lakes, or reservoirs to meet various human needs, including drinking-water supply, irrigation, and industrial use. Includes drinking-water reservoirs themselves

CREATE TEMPORARY VIEW class_350 AS (
    SELECT h3_index,
    3 AS lu_code_primary,
    5 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN topo50_hydro_and_reservoirs.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[topo50_hydro_and_reservoirs.source_data]::TEXT[],
            topo50_hydro_and_reservoirs.source_date,
            topo50_hydro_and_reservoirs.source_scale
        )::nzlum_type
        WHEN (
            linz_dvr_utility_.h3_index IS NOT NULL
            AND linz_crosl_.h3_index IS NOT NULL
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
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
            )::int4range
        )::nzlum_type
        WHEN linz_crosl_.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[linz_crosl_.source_data]::TEXT[],
            linz_crosl_.source_date,
            linz_crosl_.source_scale
        )::nzlum_type
        WHEN (
            linz_dvr_utility_.actual_property_use ~ '6[01234678]'
            OR linz_dvr_utility_.actual_property_use = '06'
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN linz_dvr_utility_.improvements_evidence
                THEN 1
                ELSE 2
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[linz_dvr_utility_.source_data]::TEXT[],
            linz_dvr_utility_.source_date,
            linz_dvr_utility_.source_scale
        )::nzlum_type
        WHEN hail_electric.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN hail_category_count = 1
                    THEN 1
                ELSE 4 -- Less confidence when there is a mixed HAIL classification
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[hail_electric.source_data]::TEXT[],
            hail_electric.source_date,
            hail_electric.source_scale
        )::nzlum_type
    END AS nzlum_type
    FROM (
        SELECT *
        FROM linz_crosl_
        WHERE managed_by IN (
            'Transpower New Zealand Limited',
            'Genesis Energy Limited',
            'Meridian Energy Limited',
            'Mighty River Power Limited - Grantee',
            'Watercare Services Limited'
        ) OR statutory_actions ~* '\m(substation|electricity|water\s?power|pump\s?station|water\s?works)\M'
    ) AS linz_crosl_
    FULL OUTER JOIN (
        SELECT *,
        CASE
            WHEN improvements_description ~ '\m(RESERVOIRS?)\M'
            THEN TRUE
            ELSE FALSE
        END AS improvements_evidence
        FROM linz_dvr_
        WHERE
            actual_property_use ~ '6[01234678]' -- 65 is sanitary (better as 3.8.X) and 69 is vacant (3.9.X)
            OR actual_property_use = '06'
            OR category ~ '^U[CEGT]' -- UP (postboxes) and UR (rail network corridors) excluded
    ) AS linz_dvr_utility_ USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,
        'LINZ' AS source_data,
        daterange(
            '2011-05-22'::DATE,
            '2025-01-02'::DATE,
            '[]'
        ) AS source_date,
        '[1,100]'::int4range AS source_scale
        FROM topo50_lake
        JOIN topo50_lake_h3 USING (ogc_fid)
        WHERE lake_use IN (
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
    ) AS topo50_hydro_and_reservoirs USING (h3_index)
    FULL OUTER JOIN (
        SELECT *
        FROM hail
        WHERE hail_category_ids @> ARRAY[
            'B4' -- Power stations, substations or switchyards
        ]
    ) AS hail_electric USING (h3_index)
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

-- HAIL for electricak substations etc. that may otherwise be absent