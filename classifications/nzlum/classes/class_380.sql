-- Waste treatment and disposal

-- This land-use type includes land uses dedicated to managing and processing various types of waste materials, primarily associated with industrial and urban activities, including solid waste, wastewater, and stormwater, to mitigate environmental impacts and protect public health.

--     Landfills – designated areas for the disposal of solid waste, typically where waste materials are deposited, compacted, and covered with soil or other materials to reduce environmental contamination and control emissions. Landfill gas recovery systems (which generate electricity through the burning of methane in landfill gas) should still be classified as part of a landfill land use. Includes all class 1 to 5 landfills (e.g. landfills designed to receive inert construction materials are also included in this category).

--     Transfer stations and recycling facilities – facilities where solid waste is collected, sorted, processed, and prepared for recycling or transfer to landfills or other disposal sites, aiming to minimise waste generation and promote resource recovery.

--     Municipal wastewater – wastewater generated from residential, commercial, and industrial activities within urban areas, requiring treatment to remove contaminants before discharge into water bodies or reuse for irrigation or other purposes. Includes municipal wastewater ponds and sewerage pipelines.

--     Wastewater treatment – land application – areas where treated municipal wastewater is applied onto land surfaces for beneficial reuse, such as irrigation of agricultural crops, recharging groundwater aquifers, or enhancing soil fertility, following appropriate treatment processes to ensure environmental safety. This is often likely to be an ancillary use.

--     Stormwater management – infrastructure aimed at controlling and mitigating the impacts of stormwater runoff, including detention basins, drainage systems, retention ponds, and green infrastructure (rain gardens, wetlands), to prevent flooding, erosion, and pollution of water bodies.

CREATE TEMPORARY VIEW class_380 AS (
    SELECT h3_index,
    3 AS lu_code_primary,
    8 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN topo50_landfill.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[topo50_landfill.source_data]::TEXT[],
            topo50_landfill.source_date,
            topo50_landfill.source_scale,
            NULL
        )::nzlum_type
        WHEN topo50_pond.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[topo50_pond.source_data]::TEXT[],
            topo50_pond.source_date,
            topo50_pond.source_scale,
            NULL
        )::nzlum_type
        WHEN linz_dvr_sanitary.h3_index IS NOT NULL
        THEN
            CASE
                WHEN (
                        linz_dvr_sanitary.actual_property_use IN (
                        '06', -- Multi-use at primary level - utility services
                        '60', -- Multi-use within utility services
                        '65' -- Utility services - sanitary
                    )
                    OR linz_dvr_sanitary.improvements_evidence IS TRUE
                )
                THEN ROW(
                    ARRAY[]::TEXT[], -- lu_code_ancillary
                    CASE
                        WHEN linz_dvr_sanitary.improvements_evidence IS TRUE AND category_evidence IS TRUE
                        THEN 1
                        WHEN linz_dvr_sanitary.improvements_evidence IS TRUE
                        THEN 2
                        WHEN linz_dvr_sanitary.category_evidence IS TRUE
                        THEN 3
                        WHEN lcdb_mines_and_dumps.h3_index IS NOT NULL
                        THEN 3
                        ELSE 8
                    END,
                    ARRAY[]::TEXT[], -- commod
                    ARRAY[]::TEXT[], -- manage
                    ARRAY[linz_dvr_sanitary.source_data, lcdb_mines_and_dumps.source_data]::TEXT[],
                    range_merge(datemultirange(
                        VARIADIC ARRAY_REMOVE(ARRAY[
                            lcdb_mines_and_dumps.source_date,
                            linz_dvr_sanitary.source_date
                        ], NULL)
                    ))::daterange, -- source_date
                    range_merge(int4multirange(
                        VARIADIC ARRAY_REMOVE(ARRAY[
                            lcdb_mines_and_dumps.source_scale,
                            linz_dvr_sanitary.source_scale
                        ], NULL)
                    ))::int4range, -- source_scale
                    NULL
                )::nzlum_type
                WHEN lcdb_mines_and_dumps.h3_index IS NOT NULL
                THEN ROW(
                    ARRAY[]::TEXT[], -- lu_code_ancillary
                    CASE
                        WHEN linz_dvr_sanitary.category_evidence IS TRUE
                        THEN 4
                        ELSE 8
                    END,
                    ARRAY[]::TEXT[], -- commod
                    ARRAY[]::TEXT[], -- manage
                    ARRAY[linz_dvr_sanitary.source_data, lcdb_mines_and_dumps.source_data]::TEXT[],
                    range_merge(datemultirange(
                        VARIADIC ARRAY_REMOVE(ARRAY[
                            lcdb_mines_and_dumps.source_date,
                            linz_dvr_sanitary.source_date
                        ], NULL)
                    ))::daterange, -- source_date
                    range_merge(int4multirange(
                        VARIADIC ARRAY_REMOVE(ARRAY[
                            lcdb_mines_and_dumps.source_scale,
                            linz_dvr_sanitary.source_scale
                        ], NULL)
                    ))::int4range, -- source_scale
                    NULL
                )::nzlum_type
                ELSE NULL
            END
        WHEN linz_crosl_sanitary.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN linz_crosl_sanitary.large = TRUE THEN 9 -- Larger parcels, lower confidence
                ELSE 8 -- Noting that preceding cases should have captured many high confidence cases
            END,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_crosl_sanitary.source_data]::TEXT[],
            linz_crosl_sanitary.source_date,
            linz_crosl_sanitary.source_scale,
            NULL
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM (
        SELECT h3_index,
        'LINZ' AS source_data,
        daterange(
            '2011-05-22'::DATE,
            '2025-01-03'::DATE,
            '[]'
        ) AS source_date,
        '[1,100]'::int4range AS source_scale
        FROM topo50_landfill_polygons_h3
        WHERE :parent::h3index = h3_partition
    ) AS topo50_landfill
    FULL OUTER JOIN (
        SELECT h3_index,
        'LINZ' AS source_data,
        daterange(
            '2011-05-22'::DATE,
            '2025-01-03'::DATE,
            '[]'
        ) AS source_date,
        '[1,100]'::int4range AS source_scale
        FROM topo50_pond
        JOIN topo50_pond_h3 USING (ogc_fid)
        WHERE
            :parent::h3index = h3_partition
            AND pond_use IN (
                'sewage',
                'sewage treatment',
                'oxidation'
                -- TODO settling, sludge?
                -- TODO join to parcel?
            )
    ) topo50_pond USING (h3_index)
    FULL OUTER JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale,
            actual_property_use,
            CASE
                WHEN improvements_description ~ '\m(SEWER(AGE)?\s?PONDS?|(OX|OXIDATION)\s?PONDS?)\M'
                THEN TRUE
                ELSE FALSE
            END AS improvements_evidence,
            CASE
                WHEN category ~ '^UC' -- Utility assets - civic, including storm water, sewerage, and water reticulation
                THEN TRUE
                ELSE FALSE
            END AS category_evidence
        FROM linz_dvr_
        WHERE
            actual_property_use IN (
                '06', -- Multi-use at primary level - utility services
                '60', -- Multi-use within utility services
                '65' -- Utility services - sanitary
            )
            OR improvements_description ~ '\m(SEWER(AGE)?\s?PONDS?|(OX|OXIDATION)\s?PONDS?)\M'
            OR category ~ '^UC'
    ) AS linz_dvr_sanitary USING (h3_index)
    FULL OUTER JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale,
        CASE WHEN area_ha > 400 THEN TRUE ELSE FALSE END AS large
        FROM linz_crosl_
        WHERE statutory_actions ~* '\m(rubbish\sdump|landfill|waste\srecovery|soild\swaste|refuse\stransfer(\sstation)?|sanitary|recyling|(waste|storm)\s?water(\sretention)?|sew(er)?(age)?|drainage)\M'
    ) AS linz_crosl_sanitary USING (h3_index)
    FULL OUTER JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale
        FROM lcdb_
        WHERE Class_2018 = 6 -- Surface Mine or Dump
    ) AS lcdb_mines_and_dumps USING (h3_index)
)

-- LINZ landfill polyogons
-- LINZ ponds with relevant pond_use categories

-- crosl stat actions "rubbish dump", "refuse transfer station", "sanitary" etc.
-- crosl statutory actions "pump station", "waste water", "sewage", "sewer(age)?"
-- NB "pump station" alone is not great evidence

-- DVR actual use 65 (utiltiy services - sanitary)
-- improvements_description, e.g. "SEWER POND?" "OX POND"
-- lower confidence without relevant improvements description
-- lower confidence without matching category (e.g. OX is not UC)
-- boost confidence with UC (Utility assets - civic, including storm water, sewerage, and water reticulation) -- but can still be confused with 3.5.0). All other U\w types are lower confidence for 3.8.0 

-- TODO any way of identifying detention basins, retention ponds, wetlands for stormwater?

-- TODO many ponds are unlabelled but when combined with DVR data the use is obvious, e.g. Pahiatua oxidation ponds