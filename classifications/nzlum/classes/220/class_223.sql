-- 2.2.3 Extensive dry stock
-- Pastoral grazing on modified pastures with relatively fewer inputs, lower stocking rates,
-- and lower likelihood of irrigation. Typically on hill, hard-hill, or high-country terrain.
-- Non-dairy livestock (beef, sheep, deer, goats) are the primary enterprise types.
--
-- This is the broad catch-all for modified pastoral land without strong intensive signals.
-- Where terrain data is absent (Chatham Islands, Stewart Island, and other offshore areas
-- not covered by NZLRI), cells default here rather than to 2.2.2 (Intensive dry stock),
-- since flat terrain evidence is required to qualify as intensive.
--
-- NZLRI slope E–H (moderately steep to precipitous) and LUC class 6–7 (low-capability
-- pastoral land) positively confirm extensive character. Flat terrain (slope A–C) and
-- high-capability land (LUC 1–4) are penalised, favouring 2.2.2 for those cells.
-- NZLRI coverage gaps are treated as neutral (no bonus or penalty).
--
-- Base confidence logic mirrors class_220 (Grazing modified pasture systems), adapted to
-- remove dairy-specific signals (handled by 2.2.1) and add terrain modifiers.

CREATE TEMPORARY VIEW class_223 AS (
    WITH unclamped AS (
        SELECT roi.h3_index,
        2 AS lu_code_primary,
        2 AS lu_code_secondary,
        3 AS lu_code_tertiary,
        ROW(
            CASE -- Ancillary: effluent land application
                WHEN (
                    irrigation_.ts_notes @@ to_tsquery('english', 'effluent')
                    OR dairy_effluent_discharge.h3_index IS NOT NULL
                )
                THEN ARRAY['2.7.3']::TEXT[]
                ELSE ARRAY[]::TEXT[]
            END,
            -- Base confidence from pastoral evidence
            CASE
                -- Crown Pastoral Lease: direct tenure evidence of extensive pastoral use
                WHEN cpl.h3_index IS NOT NULL
                THEN 2

                WHEN (
                    pastoral_consents.h3_index IS NOT NULL
                    OR linz_dvr_.actual_property_use IN (
                        '11', -- Dairy
                        '12', -- Stock finishing
                        '14'  -- Store livestock
                    )
                )
                THEN
                    CASE
                        WHEN dairy_effluent_discharge.animal_count > 0 THEN 1
                        WHEN linz_dvr_.category ~ '^D' THEN 1
                        WHEN pasture_practices.h3_index IS NOT NULL THEN 1
                        WHEN (
                            winter_forage_.CertRating = 3
                            OR (winter_forage_.CertRating = 1 AND winter_forage_.CR1Case = 2)
                        ) THEN 1
                        WHEN winter_forage_.CertRating = 2 THEN 2
                        ELSE 3
                    END

                WHEN (
                    (
                        linz_dvr_.actual_property_use = '16' -- Specialist livestock
                        AND linz_dvr_.category ~ '^(D|P|SD)'
                    ) OR (
                        linz_dvr_.actual_property_use = '19' -- Vacant within rural industry
                    ) OR linz_dvr_.category ~ '^(D|P)'
                    OR (winter_forage_.CertRating = 1 AND winter_forage_.CR1Case = 2)
                    OR pasture_practices.h3_index IS NOT NULL
                )
                THEN 5

                WHEN (
                    linz_dvr_.actual_property_use IN (
                        '13', -- Arable farming
                        '16'  -- Specialist livestock
                    ) OR (
                        linz_dvr_.actual_property_use = '17' -- Forestry
                        AND linz_dvr_.category !~ '^(FE|FP|FU)'
                    ) OR linz_dvr_.category ~ '^(LV|LB)'
                )
                THEN 8

                WHEN (
                    linz_dvr_.actual_property_use IN (
                        '15', -- Market gardens and orchards
                        '17', -- Forestry
                        '18'  -- Mineral extraction
                    )
                    OR linz_dvr_.category ~ '^(LI|SD|SH|SX)'
                    OR winter_forage_.h3_index IS NOT NULL
                )
                THEN 11

                -- Baseline for any remaining grassland
                WHEN lum_.lucid_2020 IN (
                    '75 - Grassland - High producing',
                    '76 - Grassland - Low producing'
                )
                THEN 14 -- >12 to account for modifier

                ELSE NULL
            END
            + -- Landcover modifier
            CASE
                WHEN topo50_pond_h3.h3_index IS NOT NULL
                THEN 1 -- Negative evidence
                WHEN (
                    lum_.lucid_2020 IN (
                        '75 - Grassland - High producing',
                        '76 - Grassland - Low producing'
                    )
                    AND lum_.subid_2020 <> '504 - Ungrazed'
                )
                THEN -2 -- Very positive evidence
                WHEN lum_.lucid_2020 IN (
                    '71 - Natural Forest',
                    '72 - Planted Forest - Pre 1990',
                    '73 - Post 1989 Forest'
                )
                THEN NULL -- Forested; impossible
                WHEN (
                    topo50_sand_h3.h3_index IS NOT NULL
                    OR topo50_land_h3.h3_index IS NULL
                ) THEN NULL -- Non-terrestrial; impossible
                WHEN lcdb_.h3_index IS NOT NULL
                THEN 0 -- Neutral evidence
                ELSE 1 -- Negative evidence
            END
            + CASE -- Irrigation modifier
                WHEN irrigation_.status = 'Current' AND irrigation_.confidence IN ('High', 'Medium')
                THEN -2 -- Very positive evidence
                WHEN irrigation_.h3_index IS NOT NULL
                THEN -1 -- Somewhat positive evidence
                ELSE 0
            END
            + CASE -- Lower confidence for land in hydro parcel
                WHEN hydro_parcels_h3.h3_index IS NOT NULL
                THEN 2
                ELSE 0
            END
            + CASE -- Exclude or penalise very low capability land from NZLRI
                WHEN nzlri_lowcapability.lcorrclass = 0 THEN NULL -- Impossible
                WHEN nzlri_lowcapability.lcorrclass = 8 THEN 12
                WHEN nzlri_lowcapability.lcorrclass = 7 THEN 1
                ELSE 0
            END
            + CASE -- Lower confidence for afforestation consents
                WHEN consents_forestry.afforestation_flag IS TRUE THEN 3
                ELSE 0
            END
            + CASE -- LUM dairy subid contradicts extensive dry stock classification
                WHEN lum_.subid_2020 = '502 - Grazed - dairy' THEN 4
                ELSE 0
            END
            + -- NZLRI slope: steep/very steep terrain confirms extensive character;
              -- flat terrain is penalised (likely intensive). NULL = no coverage, treat as neutral.
            CASE
                WHEN left(nzlri_slope_.slope, 1) IN ('E', 'F', 'G', 'H') THEN -1 -- Steep: confirms extensive
                WHEN left(nzlri_slope_.slope, 1) = 'D' THEN 0                     -- Strongly rolling: neutral
                WHEN left(nzlri_slope_.slope, 1) IN ('A', 'B', 'C') THEN 2        -- Flat/rolling: suggests intensive
                ELSE 0 -- No NZLRI coverage (Chathams, Stewart Island, etc.): neutral
            END
            + -- NZLRI LUC: low-capability land confirms extensive character;
              -- high-capability/arable land is penalised. NULL = no coverage, treat as neutral.
            CASE
                WHEN (regexp_match(nzlri_luc_.luc, '^\d'))[1]::int IN (6, 7) THEN -1 -- Low capability pastoral: confirms extensive
                WHEN (regexp_match(nzlri_luc_.luc, '^\d'))[1]::int = 5 THEN 0         -- Moderate pastoral: neutral
                WHEN (regexp_match(nzlri_luc_.luc, '^\d'))[1]::int BETWEEN 1 AND 4 THEN 2 -- Capable/arable: suggests intensive
                WHEN (regexp_match(nzlri_luc_.luc, '^\d'))[1]::int IN (0, 8) THEN 6   -- Extreme/impossible land
                ELSE 0 -- No coverage: neutral
            END
            + CASE -- Residential zoning contradicts commercial pastoral use
                WHEN linz_dvr_."zone" ~ '^9' THEN 3
                ELSE 0
            END,
            (
                CASE
                    WHEN pastoral_consents.h3_index IS NOT NULL
                    THEN pastoral_consents.commod::TEXT[] -- includes 'cattle dairy' for wintering-off / run-off blocks
                    WHEN linz_dvr_.category ~ '^SD' OR linz_dvr_.improvements_description ~ '\bDEER SHED(S)?\b'
                    THEN ARRAY['deer']::TEXT[]
                    ELSE ARRAY[]::TEXT[]
                END
            ) || COALESCE(pasture_practices.commod, ARRAY[]::TEXT[]), -- commod
            ARRAY_REMOVE(ARRAY[irrigation_.manage], NULL)::TEXT[]
            || COALESCE(dairy_effluent_discharge.manage, ARRAY[]::TEXT[])
            || COALESCE(ARRAY[winter_forage_.manage]::TEXT[], ARRAY[]::TEXT[])
            || COALESCE(pasture_practices.manage, ARRAY[]::TEXT[]), -- manage
            ARRAY[
                linz_dvr_.source_data,
                lcdb_.source_data,
                lum_.source_data,
                irrigation_.source_data,
                dairy_effluent_discharge.source_data,
                winter_forage_.source_data,
                nzlri_lowcapability.source_data,
                consents_forestry.source_data,
                pasture_practices.source_data,
                pastoral_consents.source_data,
                nzlri_slope_.source_data,
                nzlri_luc_.source_data
            ]::TEXT[],
            range_merge(datemultirange(VARIADIC ARRAY_REMOVE(ARRAY[
                linz_dvr_.source_date,
                lcdb_.source_date,
                lum_.source_date,
                irrigation_.source_date,
                dairy_effluent_discharge.source_date,
                winter_forage_.source_date,
                nzlri_lowcapability.source_date,
                consents_forestry.source_date,
                pasture_practices.source_date,
                pastoral_consents.source_date,
                nzlri_slope_.source_date,
                nzlri_luc_.source_date
            ], NULL)))::daterange,
            range_merge(int4multirange(VARIADIC ARRAY_REMOVE(ARRAY[
                linz_dvr_.source_scale,
                lcdb_.source_scale,
                lum_.source_scale,
                irrigation_.source_scale,
                dairy_effluent_discharge.source_scale,
                winter_forage_.source_scale,
                nzlri_lowcapability.source_scale,
                consents_forestry.source_scale,
                pasture_practices.source_scale,
                pastoral_consents.source_scale,
                nzlri_slope_.source_scale,
                nzlri_luc_.source_scale
            ], NULL)))::int4range,
            NULL
        )::nzlum_type AS nzlum_type
        FROM roi
        LEFT JOIN (
            SELECT
                h3_index, source_data, source_date, source_scale,
                improvements_description, actual_property_use, category, "zone"
            FROM linz_dvr_
            WHERE linz_dvr_.actual_property_use ~ '^1' -- Rural industry
               OR linz_dvr_.actual_property_use = '01' -- Multi-use at primary level, rural-industry
        ) linz_dvr_ ON roi.h3_index && linz_dvr_.h3_index
        LEFT JOIN (
            SELECT h3_index, source_data, source_date, source_scale
            FROM lcdb_
            WHERE Class_2023 IN (
                41, -- Low Producing Grassland
                44, -- Depleted Grassland
                55 -- Sub-Alpine Shrubland
            )
        ) AS lcdb_ ON roi.h3_index && lcdb_.h3_index
        LEFT JOIN (
            SELECT
                h3_index, source_data, source_date, source_scale,
                confidence, manage, "status", ts_notes
            FROM irrigation_
            WHERE (
                ts_notes IS NULL
                OR NOT ts_notes @@ to_tsquery('english', 'sports & field | golf & course')
            ) AND irrigation_.irrigation_type NOT LIKE 'Drip%'
        ) irrigation_ ON roi.h3_index && irrigation_.h3_index
        LEFT JOIN (
            SELECT h3_index, source_data, source_date, source_scale, manage, animal_count
            FROM dairy_effluent_discharge
        ) AS dairy_effluent_discharge ON roi.h3_index && dairy_effluent_discharge.h3_index
        LEFT JOIN (
            SELECT h3_index, source_data, source_date, source_scale, manage, CertRating, CR1Case
            FROM winter_forage_
        ) AS winter_forage_ ON roi.h3_index && winter_forage_.h3_index
        LEFT JOIN (
            SELECT h3_index, source_date, source_scale, source_data, commod
            FROM pastoral_consents
        ) AS pastoral_consents ON roi.h3_index && pastoral_consents.h3_index
        LEFT JOIN lum_ ON roi.h3_index && lum_.h3_index
        LEFT JOIN (
            SELECT h3_index
            FROM topo50_sand_h3
            WHERE :parent::h3index = h3_partition
            UNION ALL
            SELECT h3_index
            FROM topo50_chatham_sand_h3
            WHERE :parent::h3index = h3_partition
        ) AS topo50_sand_h3 ON roi.h3_index && topo50_sand_h3.h3_index
        LEFT JOIN (
            SELECT h3_index
            FROM topo50_land_h3
            WHERE :parent::h3index = h3_partition
        ) AS topo50_land_h3 ON roi.h3_index && topo50_land_h3.h3_index
        LEFT JOIN (
            SELECT h3_index
            FROM topo50_pond_h3
            WHERE :parent::h3index = h3_partition
        ) AS topo50_pond_h3 ON roi.h3_index && topo50_pond_h3.h3_index
        LEFT JOIN (
            SELECT h3_index
            FROM hydro_parcels_h3
            WHERE :parent::h3index = h3_partition
        ) AS hydro_parcels_h3 ON roi.h3_index && hydro_parcels_h3.h3_index
        LEFT JOIN (
            SELECT h3_index, lcorrclass, source_data, source_date, source_scale
            FROM nzlri_luc
            JOIN nzlri_luc_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
            AND lcorrclass IN (0, 7, 8)
        ) AS nzlri_lowcapability ON roi.h3_index && nzlri_lowcapability.h3_index
        LEFT JOIN (
            SELECT h3_index, source_data, source_date, source_scale, afforestation_flag
            FROM consents_forestry
        ) AS consents_forestry ON roi.h3_index && consents_forestry.h3_index
        LEFT JOIN (
            SELECT h3_index, source_data, source_date, source_scale, manage, commod
            FROM crop_maps
            WHERE manage && ARRAY['residues baled', 'crop pasture rotation', 'grazing rotational']
            OR (
                source_data = 'GDC'
                AND crop && ARRAY['Pasture', 'Pasture/Unused']
            )
        ) AS pasture_practices ON roi.h3_index && pasture_practices.h3_index
        LEFT JOIN (
            -- NZLRI slope: A (flat) through H (precipitous). Absent for offshore islands (Chathams, Stewart).
            SELECT h3_index, slope, source_data, source_date, source_scale
            FROM nzlri_slope
            JOIN nzlri_slope_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) AS nzlri_slope_ ON roi.h3_index && nzlri_slope_.h3_index
        LEFT JOIN (
            -- NZLRI LUC: text-formatted LUC code (e.g. '4e 6'). Absent for offshore islands.
            SELECT h3_index, luc, source_data, source_date, source_scale
            FROM nzlri_luc
            JOIN nzlri_luc_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) AS nzlri_luc_ ON roi.h3_index && nzlri_luc_.h3_index
        LEFT JOIN (
            -- Crown Pastoral Leases with exotic/modified/degraded pastoral land cover.
            -- CPLs with native cover (e.g. tall tussock) are captured by class_130 instead.
            -- NB: forest cover (64, 71) is excluded even on a pastoral lease title.
            SELECT south_island_pastoral_leases_h3.h3_index,
            'LINZ' AS source_data,
            daterange('2013-02-07'::DATE, '2024-11-04'::DATE, '[]') AS source_date,
            '[100,)'::int4range AS source_scale -- Boundaries are indicative only
            FROM south_island_pastoral_leases_h3
            WHERE :parent::h3index = south_island_pastoral_leases_h3.h3_partition
            AND EXISTS (
                SELECT 1 FROM lcdb_
                WHERE :parent::h3index = h3_partition
                AND Class_2023 IN (
                    40, -- High Producing Exotic Grassland
                    41, -- Low Producing Grassland (often modified)
                    44, -- Depleted Grassland (grazing impact in pastoral lease context)
                    51, -- Gorse and/or Broom
                    56  -- Mixed Exotic Shrubland
                )
                AND south_island_pastoral_leases_h3.h3_index && lcdb_.h3_index
            )
        ) AS cpl ON roi.h3_index && cpl.h3_index
        WHERE lum_.h3_index IS NOT NULL
           OR linz_dvr_.h3_index IS NOT NULL
           OR lcdb_.h3_index IS NOT NULL
           OR dairy_effluent_discharge.h3_index IS NOT NULL
           OR winter_forage_.h3_index IS NOT NULL
           OR pastoral_consents.h3_index IS NOT NULL
           OR pasture_practices.h3_index IS NOT NULL
           OR cpl.h3_index IS NOT NULL
    )
    -- Clamp final confidence to 1–12; values >12 are NULL (excluded).
    -- Uses LEAST/GREATEST rather than clamp_confidence_or_null so that intermediate
    -- calculations above 12 (e.g. LUM baseline 14 + modifier −2 = 12) are correctly included.
    SELECT
        h3_index,
        lu_code_primary,
        lu_code_secondary,
        lu_code_tertiary,
        ROW(
            (nzlum_type).lu_code_ancillary,
            CASE
                WHEN (nzlum_type).confidence IS NULL THEN NULL
                ELSE LEAST(GREATEST((nzlum_type).confidence, 1), 12)
            END,
            (nzlum_type).commod,
            (nzlum_type).manage,
            (nzlum_type).source_data,
            (nzlum_type).source_date,
            (nzlum_type).source_scale,
            (nzlum_type).comment
        )::nzlum_type AS nzlum_type
    FROM unclamped
);
