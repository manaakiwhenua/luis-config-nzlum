-- This class captures built water features associated with agricultural use.

-- Stock water – reservoirs or farm dams on agricultural land for the purpose of supplying drinking-water for stock.

-- Effluent pond – effluent ponds typically associated with dairying.

-- Water treatment – land application – land used for effluent disposal, probably an ancillary use where some form of grazing is the primary use.

-- Water treatment – wetland – constructed or natural wetlands used to improve water quality prior to discharge.

-- Irrigation reservoirs and canals – land used for water storage, management or distribution intended for agricultural purposes; artificial or natural areas allocated for irrigation for agricultural purposes.

CREATE TEMPORARY VIEW class_270 AS ( -- Water and wastewater
    SELECT h3_index,
    2 AS lu_code_primary,
    7 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN (
            unspecified_waterbodies.h3_index IS NOT NULL
            AND (
                linz_dvr_rural.h3_index IS NOT NULL
                OR lcdb_.h3_index IS NOT NULL
            )
            AND urban_rural_2025_.h3_index IS NOT NULL
        ) 
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[
                unspecified_waterbodies.source_data,
                linz_dvr_rural.source_data
            ]::TEXT,
            range_merge(
                unspecified_waterbodies.source_date,
                linz_dvr_rural.source_date
            ),
            range_merge(
                unspecified_waterbodies.source_scale,
                linz_dvr_rural.source_scale
            )
        )::nzlum_type
        WHEN lakes.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[lakes.source_data]::TEXT[],
            lakes.source_date,
            lakes.source_scale
        )::nzlum_type
        WHEN irrigation_canals.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[irrigation_canals.source_data]::TEXT[],
            irrigation_canals.source_date,
            irrigation_canals.source_scale
        )::nzlum_type
        WHEN dairy_effluent_discharge.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            10, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[dairy_effluent_discharge.source_data]::TEXT[],
            dairy_effluent_discharge.source_date,
            dairy_effluent_discharge.source_scale
        )::nzlum_type
    END AS nzlum_type
    FROM (
        SELECT h3_index,
        daterange(
            '2011-05-22'::DATE,
            '2025-01-02'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM (
            SELECT h3_index
            FROM topo50_pond
            JOIN topo50_pond_h3 USING (ogc_fid)
            WHERE
                pond_use IS NULL
                AND "name" IS NULL
                AND :parent::h3index = h3_partition
            UNION ALL
            SELECT h3_index
            FROM topo50_lake
            JOIN topo50_lake_h3 USING (ogc_fid)
            WHERE
                lake_use IS NULL
                AND "name" IS NULL
                AND "grp_name" IS NULL
                AND :parent::h3index = h3_partition
        ) topo50_water
    ) unspecified_waterbodies
    FULL OUTER JOIN(
        SELECT *
        FROM linz_dvr_
        WHERE actual_property_use ~ '^0?[012]' -- Rural industry or lifestyle, including multi-use at primary level, or vacant
        AND actual_property_use != '18' -- Mineral extraction
    ) linz_dvr_rural USING (h3_index)
    FULL OUTER JOIN (
        SELECT *
        FROM lcdb_
        WHERE class_2018 IN (
            '20', -- Lake or pond
            '30', -- Short-rotation cropland
            '33', -- Orchards, Vineyards or Other Perennial Crops
            '40', -- High Producing Grassland
            '41' -- Low Producing Grassland
        )
    ) AS lcdb_ USING (h3_index)
    FULL OUTER JOIN (
        SELECT *
        FROM dairy_effluent_discharge
        WHERE :parent::h3index = h3_partition
    ) AS dairy_effluent_discharge USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,
        daterange(
            '2011-05-22'::DATE,
            '2025-01-02'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_lake
        JOIN topo50_lake_h3 USING (ogc_fid)
        WHERE (
            "name" ~* '\mreservoir\M'
            AND lake_use != 'hydro-electric'
        ) OR lake_use = 'reservoir'
        AND :parent::h3index = h3_partition
    ) lakes USING (h3_index)
    FULL OUTER JOIN (
        SELECT h3_index,
        daterange(
            '2015-03-11'::DATE,
            '2024-12-19'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_canal
        JOIN topo50_canal_h3 USING (ogc_fid)
        WHERE "name" = 'Rangitata Diversion Race'
        AND :parent::h3index = h3_partition
    ) irrigation_canals USING (h3_index)
    LEFT JOIN (
        SELECT
            urban_rural_2025_h3.h3_index,
            urban_rural_2025.IUR2025_V1_00
        FROM urban_rural_2025_h3
        JOIN urban_rural_2025 USING (ogc_fid)
        WHERE urban_rural_2025.IUR2025_V1_00 IN (
            '22', -- Rural other
            '31' -- Inland water
        ) -- Rural other
    ) AS urban_rural_2025_ USING (h3_index)
);

