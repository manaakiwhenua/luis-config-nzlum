CREATE TEMPORARY VIEW class_370 AS ( -- Mining
    -- Mines
    -- Quarries
    -- Tailings
    -- Evaporation basins
    -- Extractive industry not in use
    SELECT h3_index,
    3 AS lu_code_primary,
    7 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN quarries_.status = 'disused'
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            2,
            array_remove(ARRAY[quarries_.substance]::TEXT[], NULL),
            ARRAY[]::TEXT[],
            ARRAY[quarries_.source_data]::TEXT[],
            quarries_.source_date,
            quarries_.source_scale,
            NULL
        )::nzlum_type
        WHEN (
            mines_.h3_index IS NOT NULL
            OR quarries_.h3_index IS NOT NULL
            OR evaporation_ponds_.h3_index IS NOT NULL
            OR dredge_tailings_.h3_index IS NOT NULL
            OR settling_ponds_.h3_index IS NOT NULL
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            array_remove(ARRAY[
                mines_.substance,
                quarries_.substance,
                CASE WHEN evaporation_ponds_.h3_index IS NOT NULL THEN 'salt' ELSE NULL END
            ]::TEXT[], NULL), -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY_REMOVE(ARRAY[
                quarries_.source_data,
                mines_.source_data,
                evaporation_ponds_.source_data,
                dredge_tailings_.source_data,
                settling_ponds_.source_data
            ], NULL)::TEXT[], -- source_data
            range_merge(datemultirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    quarries_.source_date,
                    mines_.source_date,
                    evaporation_ponds_.source_date,
                    dredge_tailings_.source_date,
                    settling_ponds_.source_date
                ], NULL)
            ))::daterange,
            range_merge(int4multirange(
                VARIADIC ARRAY_REMOVE(ARRAY[            
                    mines_.source_scale,
                    quarries_.source_scale,
                    evaporation_ponds_.source_scale,
                    dredge_tailings_.source_scale,
                    settling_ponds_.source_scale
                ], NULL)
            ))::int4range,
            NULL
        )::nzlum_type
        WHEN (
            linz_dvr_.actual_property_use IN ('10', '18', '78')
            AND linz_dvr_.improvements_description ~ '\mQUARRY\M'
        )
        THEN ROW(
            ARRAY[]::TEXT[],
            2,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale,
            NULL
        )::nzlum_type
        WHEN linz_dvr_.actual_property_use = '18'
        THEN ROW(
            ARRAY[]::TEXT[],
            3,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale,
            NULL
        )::nzlum_type
        WHEN linz_dvr_.actual_property_use <> '18' AND linz_dvr_.category ~ '^M' -- Properties not used primarily for mining, but it is noted as the best use
        THEN ROW(
            ARRAY[]::TEXT[],
            4,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale,
            NULL
        )::nzlum_type
        WHEN linz_dvr_.category ~ '^M' -- Anything remaining of which the best use is noted as mining
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            11,
            CASE
                WHEN linz_dvr_.category = 'MC' THEN ARRAY['coal']::TEXT[]
                WHEN linz_dvr_.category = 'MG' THEN ARRAY['gas']::TEXT[]
                WHEN linz_dvr_.category = 'ML' THEN ARRAY['limestone']::TEXT[]
                WHEN linz_dvr_.category = 'MO' THEN ARRAY['oil']::TEXT[]
                WHEN linz_dvr_.category = 'MP' THEN NULL -- TODO precious metal mines
                WHEN linz_dvr_.category = 'MR' THEN NULL -- TODO rock, shingle, or sand pits and extraction
                WHEN linz_dvr_.category = 'MX' THEN NULL -- TODO multiple mining activities, or otherwise not specified
                ELSE NULL
            END,
            ARRAY[]::TEXT[],
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale,
            NULL
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    -- commodity type? mines.substance = ironsand,coal, gold
    -- commodity type ? evaporation_ponds_.pond_use = evaporation, i.e. salt, but also caustic soda and gypsum according to https://www.marlboroughonline.co.nz/marlborough/information/geography/lakes/lake-grassmere/
    -- management practice? mines.visibility = opencast
    -- quarries.status = NULL,disused
    -- quarries.substance = NULL,clay,gravel,lime,limestone,metal,shingle,silica sand,stone,zeolite
    FROM (
        SELECT
            h3_index,
            substance,
            daterange('2011-05-22'::DATE, '2025-03-06'::DATE, '[]') AS source_date,
            'LINZ' AS source_data,
            '[60,100)'::int4range AS source_scale
        FROM topo50_mines
        JOIN topo50_mines_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS mines_
    FULL OUTER JOIN (
        SELECT
            h3_index,
            "status",
            substance,
            daterange('2011-05-22'::DATE, '2024-01-03'::DATE, '[]') AS source_date,
            'LINZ' AS source_data,
            '[60,100)'::int4range AS source_scale
        FROM topo50_quarries
        JOIN topo50_quarries_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        
        UNION ALL

        SELECT
            h3_index,
            "status",
            substance,
            daterange(
                '2011-05-22'::DATE,
                '2024-03-20'::DATE,
                '[]'
            ) AS source_date,
            'LINZ' AS source_data,
            '[60,100)'::int4range AS source_scale
        FROM topo50_chatham_quarries
        JOIN topo50_chatham_quarries_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS quarries_ USING (h3_index)
    FULL OUTER JOIN (
        -- NB topo50 pond; pond_use = evaporation, i.e. Lake Grassmere solar salt production
        SELECT
            h3_index,
            daterange('2011-05-22'::DATE, '2025-01-02'::DATE, '[]') AS source_date,
            'LINZ' AS source_data,
            '[60,100)'::int4range AS source_scale
        FROM topo50_pond
        JOIN topo50_pond_h3 USING (ogc_fid)
        WHERE
            :parent::h3index = h3_partition
            AND pond_use = 'evaporation'
    ) AS evaporation_ponds_ USING (h3_index)
    FULL OUTER JOIN (
        SELECT
            h3_index,
            daterange('2011-05-22'::DATE, '2025-01-02'::DATE, '[]') AS source_date,
            'LINZ' AS source_data,
            '[60,100)'::int4range AS source_scale
        FROM topo50_pond
        JOIN topo50_pond_h3 USING (ogc_fid)
        WHERE
            :parent::h3index = h3_partition
            AND pond_use = 'settling'
    ) AS settling_ponds_ USING (h3_index)
    FULL OUTER JOIN (
        SELECT
            h3_index,
            daterange('2011-05-22'::DATE, '2024-12-20'::DATE, '[]') AS source_date,
            'LINZ' AS source_data,
            '[60,100)'::int4range AS source_scale
        FROM topo50_dredge_tailing_centrelines
        JOIN topo50_dredge_tailing_centrelines_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS dredge_tailings_ USING (h3_index)
    FULL OUTER JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale,
            actual_property_use,
            category,
            improvements_description
        FROM linz_dvr_
        WHERE
            category ~ '^M'
            OR actual_property_use IN (
                '10', -- Rural industry, multiple use within rural industry
                '18', -- Rural industry, mineral extraction
                '78' -- Industrial, depots and yards
            )
            OR improvements_description ~ '/mQUARRY/M'
    ) AS linz_dvr_ USING (h3_index)
);

-- Use DVR category for commodity
-- category M% -- Mining and other mineral extraction sites of all descriptions
-- MC (Coal fields)
-- MG (gas)
-- ML (limestone quarries)
-- MO (oilfields)
-- MP (precious metal mining sites)
-- MR (rock, shingle, or sand pits and extraction)
-- MX (multiple mining activities or not otherwise specified)


-- TODO consider LCDB quarries; but NB that it's "mines or dumps"
-- TODO consider LUM?
-- TODO use CROSL?