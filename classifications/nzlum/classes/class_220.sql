CREATE TEMPORARY VIEW class_220 AS ( -- Grazing modified pasture systems
    WITH unclamped_confidence AS (
        SELECT h3_index,
        2 AS lu_code_primary,
        2 AS lu_code_secondary,
        0 AS lu_code_tertiary,
        ROW(
            CASE
                WHEN (
                    irrigation_.ts_notes @@ to_tsquery('english', 'effluent')
                    OR dairy_effluent_discharge.h3_index IS NOT NULL
                )
                THEN ARRAY['2.7.3']::TEXT[] -- Applying effluent to land as irrigation
                ELSE ARRAY[]::TEXT[]
            END, -- lu_code_ancillary
            CASE
                WHEN linz_dvr_.actual_property_use IN (
                    '11', -- Dairy
                    '12', -- Stock finishing
                    '14' -- Store livestock
                )
                THEN
                    CASE
                        WHEN dairy_effluent_discharge.ActualAnimalNumbers_int > 0
                        THEN 1
                        WHEN (
                            winter_forage_2022.CertRating = 3 -- Good
                            OR (
                                winter_forage_2022.CertRating = 1
                                AND winter_forage_2022.CR1Case = 2 -- Heavily grazed pasture
                            )
                        )
                        THEN 1
                        WHEN winter_forage_2022.CertRating = 2 -- Medium
                        THEN 2
                        ELSE 3
                    END

                WHEN (
                    (
                        linz_dvr_.actual_property_use = '16' -- Specialist livestock
                        AND linz_dvr_.category ~ '^(D|P|SD)' -- Incl. specialist livestock but attempting to filter only for pastoral specialities
                    ) OR (
                        linz_dvr_.actual_property_use = '19' -- Vacant within rural industry
                    ) OR linz_dvr_.category ~ '^(D|P)'
                    OR (
                        winter_forage_2022.CertRating = 1
                        AND winter_forage_2022.CR1Case = 2 -- Heavily grazed pasture
                    )
                )
                THEN 5

                WHEN (
                    linz_dvr_.actual_property_use IN (
                        '13', -- Arable farming
                        '16' -- Specialist livestock (usu. not pastoral
                    ) OR (
                            linz_dvr_.actual_property_use = '17' -- Forestry
                            AND linz_dvr_.category !~ '^(FE|FP|FU)' -- (Negated regex match) Allow for vacant forestry as low evidence
                    ) OR linz_dvr_.category ~ '^(LV|LB)'
                )
                THEN 8

                WHEN (
                        linz_dvr_.actual_property_use IN (
                        '15', -- Market gardens and orchards
                        '17', -- Forestry
                        '18' -- Mineral extraction
                    )
                    OR linz_dvr_.category ~ '^(LI|SD|SH|SX)'
                    OR winter_forage_2022.h3_index IS NOT NULL
                )
                THEN 11

                -- Baseline for any remaining grassland
                WHEN lum_.lucid_2020 IN (
                    '75 - Grassland - High producing',
                    '76 - Grassland - Low producing'
                )
                THEN 14 -- >12 to account for modifier
                ELSE NULL -- No other possibilities permitted
            END
            + -- Add modifier for landcover
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
                WHEN lcdb_.Class_2018 IN (10, 12, 15, 16, 20, 21, 30, 41, 44, 51, 52, 55, 56, 58, 80, 81, 64)
                THEN 0 -- Neutral evidence
                ELSE 1 -- Negative evidence
            END
            +
            CASE -- Lower confidence for land in hydro parcel
                WHEN hydro_parcels_h3.h3_index IS NOT NULL
                THEN 2
                ELSE 0
            END
            +
            CASE -- Lower confidence for extremely low capability land according to LUC
                WHEN nzlri_lowcapability.lcorrclass = 0
                THEN NULL -- Complete exclude the possibility
                WHEN nzlri_lowcapability.lcorrclass = 8
                THEN 12 -- Extremely unlikely; high penalty
                WHEN nzlri_lowcapability.lcorrclass = 7
                THEN 1 -- Small additional penalty
                ELSE 0
            END
            +
            CASE -- Lower confidence for afforestation consents
                WHEN consents_forestry.afforestation_flag IS TRUE
                THEN 3
                ELSE 0
            END, -- confidence
            -- TODO specific commodities per case; this is just placeholder
            -- TODO in this case, it will require moving the modifier to the later part of the query
            CASE
                WHEN (
                    linz_dvr_.actual_property_use = '11'
                    OR lum_.subid_2020 = '502 - Grazed - dairy'
                    OR dairy_effluent_discharge.h3_index IS NOT NULL
                )
                THEN ARRAY['cattle dairy']::TEXT[]
                WHEN linz_dvr_.category ~ '^SD' OR linz_dvr_.improvements_description ~ '\bDEER SHED(S)?\b'
                THEN ARRAY['deer']::TEXT[]
                ELSE ARRAY[]::TEXT[]
            END, -- commod
            ARRAY_REMOVE(ARRAY[
                GREATEST(
                    irrigation_.manage,
                    dairy_effluent_discharge.effluent_irrigation_type
                ), -- GREATEST is used to handle the {irrigation,irrigation spray} case, to retain only the option with the most detail
                winter_forage_2022.manage
            ]::TEXT[], NULL), -- manage
            ARRAY[
                linz_dvr_.source_data,
                lcdb_.source_data,
                lum_.source_data,
                irrigation_.source_data,
                dairy_effluent_discharge.source_data,
                winter_forage_2022.source_data,
                nzlri_lowcapability.source_data,
                consents_forestry.source_data
            ]::TEXT[], -- source_data
            range_merge(datemultirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    linz_dvr_.source_date,
                    lcdb_.source_date,
                    lum_.source_date,
                    irrigation_.source_date,
                    dairy_effluent_discharge.source_date,
                    winter_forage_2022.source_date,
                    nzlri_lowcapability.source_date,
                    consents_forestry.source_date
                ], NULL)
            ))::daterange, -- source_date
            range_merge(int4multirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    linz_dvr_.source_scale,
                    lcdb_.source_scale,
                    lum_.source_scale,
                    irrigation_.source_scale,
                    dairy_effluent_discharge.source_scale,
                    winter_forage_2022.source_scale,
                    nzlri_lowcapability.source_scale,
                    consents_forestry.source_scale
                ], NULL)
            ))::int4range -- source_scale
        )::nzlum_type AS nzlum_type
        FROM lum_
        FULL OUTER JOIN (
            SELECT * FROM linz_dvr_
            WHERE linz_dvr_.actual_property_use ~ '^1' -- Rural industry
            OR linz_dvr_.actual_property_use = '01' -- Multi-use at primary level, rural-industry
        ) linz_dvr_ USING (h3_index)
        FULL OUTER JOIN lcdb_ USING (h3_index)
        FULL OUTER JOIN (
            SELECT *
            FROM irrigation_
            WHERE (
                ts_notes IS NULL -- Very important
                OR NOT ts_notes @@ to_tsquery('english', 'sports & field | golf & course')
            ) AND "type" NOT LIKE 'Drip%'
        ) irrigation_ USING (h3_index)
        FULL OUTER JOIN (
            SELECT *
            FROM dairy_effluent_discharge
            WHERE :parent::h3index = h3_partition
        ) AS dairy_effluent_discharge USING (h3_index)
        FULL OUTER JOIN (
            SELECT *
            FROM winter_forage_2022
            JOIN winter_forage_2022_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) AS winter_forage_2022 USING (h3_index)
        LEFT JOIN (
            SELECT *
            FROM topo50_sand_h3
            WHERE :parent::h3index = h3_partition 
            UNION ALL
            SELECT * 
            FROM topo50_chatham_sand_h3
            WHERE :parent::h3index = h3_partition
        ) AS topo50_sand_h3 USING (h3_index)
        LEFT JOIN topo50_land_h3 USING (h3_index)
        LEFT JOIN topo50_pond_h3 USING (h3_index)
        LEFT JOIN hydro_parcels_h3 USING (h3_index)
        LEFT JOIN (
            SELECT h3_index, lcorrclass, source_data, source_date, source_scale
            FROM nzlri_lowcapability
            JOIN nzlri_lowcapability_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition 
        ) AS nzlri_lowcapability USING (h3_index)
        LEFT JOIN consents_forestry USING (h3_index)
    )
    SELECT
        h3_index,
        lu_code_primary,
        lu_code_secondary,
        lu_code_tertiary,
        ROW(
            (nzlum_type).lu_code_ancillary,
            -- Clamp the confidence value, preserving nulls
            CASE
                WHEN (nzlum_type).confidence IS NULL THEN NULL
                ELSE LEAST(GREATEST((nzlum_type).confidence, 1), 12)
            END,
            (nzlum_type).commod,
            (nzlum_type).manage,
            (nzlum_type).source_data,
            (nzlum_type).source_date,
            (nzlum_type).source_scale
        )::nzlum_type AS nzlum_type
    FROM unclamped_confidence
);

-- TODO CHM
-- Lower confidence with canopy height
-- Higher confidence with no canopy (but insufficient alone)
-- TODO patch size from CHM? Small patches able to be ignored; larger patches not.

-- Note that LUM integrated ratings data, and we have more up to date information
-- Weak evidence: 74, 74/0, 78 (annual cropland)
-- Good evidence: 75 (high producing grassland), 75/0, 75/501 (winter forage), 75/502 (dairy), 75/503 (non-dairy); ditto for 76 (low producing grassland)
-- Neutral : 82 (other)
-- Negative evidence: 75/501 and  74/501 (*grassland ungrazed) and all other classes

-- MfE irrigation, considering status (i.e. current) and confidence; type as mgmt practice; notes as comment?

-- TODO NZLRI LUC lower confidence for luc 0 or 8, even a small lower confidence for class 7

-- NB CROSL does -not- directly identify crown pastoral leases

-- TODO lower confidence with hydro parcel