-- 2.2.2 Intensive dry stock
-- Non-dairy pastoral grazing on relatively capable, flat to rolling terrain.
-- Characterised by higher inputs (fertiliser, irrigation), higher stocking rates, and
-- LUC class ≤5. Typically on flat to strongly rolling slopes (NZLRI slope A–D).
-- Common enterprises include beef finishing (stock finishing APU 12), lamb finishing,
-- store livestock (APU 14), and non-lactating dairy support.
--
-- Scope note: Land used for non-lactating (dry) dairy cattle is classified here or under
-- 2.2.3 depending on terrain. Actively milked land → 2.2.1 Dairy.
--
-- The WHERE clause requires at least one positive intensive signal — NZLRI slope A–D,
-- LUC class 1–5, current irrigation, or DVR stock finishing (APU 12). Cells lacking all
-- of these default to 2.2.3 Extensive dry stock instead.
-- NZLRI coverage gaps (Chatham Islands, Stewart Island, etc.) are treated as neutral in
-- confidence modifiers; those cells can still qualify via irrigation or APU 12.
--
-- Where both 2.2.2 and 2.2.3 produce a row for the same cell, the classification with
-- lower (more certain) confidence wins through the nzlum prioritisation sort.

CREATE TEMPORARY VIEW class_222 AS (
    WITH unclamped AS (
        SELECT roi.h3_index,
        2 AS lu_code_primary,
        2 AS lu_code_secondary,
        2 AS lu_code_tertiary,
        ROW(
            CASE -- Ancillary: effluent land application
                WHEN (
                    irrigation_.ts_notes @@ to_tsquery('english', 'effluent')
                    OR dairy_effluent_discharge.h3_index IS NOT NULL
                )
                THEN ARRAY['2.7.3']::TEXT[]
                ELSE ARRAY[]::TEXT[]
            END,
            -- Base confidence from pastoral evidence (mirrors class_223 / class_220 logic)
            CASE
                -- Crown Pastoral Lease: direct tenure evidence of pastoral use.
                -- Terrain modifiers below handle the 222/223 split within CPLs.
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
                WHEN irrigation_.status = 'Current' AND irrigation_.confidence IN ('High', 'Medium')
                THEN -2 -- Very positive evidence
                WHEN irrigation_.h3_index IS NOT NULL
                THEN -1 -- Somewhat positive evidence
                WHEN lum_.lucid_2020 IN (
                    '71 - Natural Forest',
                    '72 - Planted Forest - Pre 1990',
                    '73 - Post 1989 Forest'
                )
                THEN 2 -- Negative evidence
                WHEN (
                    topo50_sand_h3.h3_index IS NOT NULL
                    OR topo50_land_h3.h3_index IS NULL
                ) THEN NULL -- Impossible
                WHEN lcdb_.h3_index IS NOT NULL
                THEN 0 -- Neutral evidence
                ELSE 1 -- Negative evidence
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
            + CASE -- LUM dairy subid contradicts intensive dry stock classification
                WHEN lum_.subid_2020 = '502 - Grazed - dairy' THEN 4
                ELSE 0
            END
            + -- NZLRI slope: flat/undulating terrain confirms intensive character;
              -- steep terrain is penalised (contradicts intensive management). NULL = no coverage, neutral.
            CASE
                WHEN left(nzlri_slope_.slope, 1) IN ('A', 'B') THEN -2 -- Flat to undulating: strongly confirms intensive
                WHEN left(nzlri_slope_.slope, 1) = 'C' THEN -1          -- Rolling: somewhat confirms intensive
                WHEN left(nzlri_slope_.slope, 1) = 'D' THEN 0           -- Strongly rolling: neutral
                WHEN left(nzlri_slope_.slope, 1) IN ('E', 'F', 'G', 'H') THEN 4 -- Steep: contradicts intensive
                ELSE 0 -- No NZLRI coverage (Chathams, Stewart Island, etc.): neutral
            END
            + -- NZLRI LUC: high-capability land confirms intensive character;
              -- low-capability land is penalised. NULL = no coverage, neutral.
            CASE
                WHEN (regexp_match(nzlri_luc_.luc, '^\d'))[1]::int BETWEEN 1 AND 3 THEN -2 -- Prime/good land: confirms intensive
                WHEN (regexp_match(nzlri_luc_.luc, '^\d'))[1]::int IN (4, 5) THEN 0         -- Moderate: neutral
                WHEN (regexp_match(nzlri_luc_.luc, '^\d'))[1]::int BETWEEN 6 AND 8 THEN 4   -- Low capability: contradicts intensive
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
            WHERE Class_2023 IN (10, 12, 15, 16, 20, 21, 30, 40, 41, 44, 51, 52, 55, 56, 58, 80, 81, 64)
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
            -- CPLs are large and heterogeneous; terrain modifiers determine whether a cell
            -- is intensive (2.2.2) or extensive (2.2.3) within the lease boundary.
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
                    44, -- Depleted Grassland
                    51, -- Gorse and/or Broom
                    56  -- Mixed Exotic Shrubland
                )
                AND south_island_pastoral_leases_h3.h3_index && lcdb_.h3_index
            )
        ) AS cpl ON roi.h3_index && cpl.h3_index
        WHERE (
            -- Pastoral evidence required
            lum_.h3_index IS NOT NULL
            OR linz_dvr_.h3_index IS NOT NULL
            OR lcdb_.h3_index IS NOT NULL
            OR dairy_effluent_discharge.h3_index IS NOT NULL
            OR winter_forage_.h3_index IS NOT NULL
            OR pastoral_consents.h3_index IS NOT NULL
            OR pasture_practices.h3_index IS NOT NULL
            OR cpl.h3_index IS NOT NULL
        ) AND (
            -- At least one positive intensive signal required; without this, default to 2.2.3.
            -- NZLRI gaps (NULL slope/luc) evaluate as non-matching, so those cells fall through to 2.2.3.
            left(nzlri_slope_.slope, 1) IN ('A', 'B', 'C', 'D')                   -- Flat to strongly rolling
            OR (regexp_match(nzlri_luc_.luc, '^\d'))[1]::int BETWEEN 1 AND 5      -- Capable land (LUC 1–5)
            OR irrigation_.status = 'Current'                                      -- Active irrigation
            OR linz_dvr_.actual_property_use = '12'                               -- Stock finishing
        )
    )
    -- Clamp final confidence to 1–12; values >12 are NULL (excluded).
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
