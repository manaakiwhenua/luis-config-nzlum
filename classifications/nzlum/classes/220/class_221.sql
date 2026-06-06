-- 2.2.1 Dairy
-- Milking platform land and dairy support land where dairy is the primary purpose.
-- Includes rotations of grazed forage crops and maize for silage; the defining feature
-- is that dairy production (lactating stock) drives the land management.
--
-- Scope note: Dry-stock grazing on the milking platform between seasons is still class 2.2.1
-- as long as the milking platform is the primary purpose. Non-lactating dairy support that is
-- permanently in dry-stock use should be classified under 2.2.2 or 2.2.3.
--
-- Primary evidence (in descending confidence):
--   dairy_effluent_discharge consents (strongest — consent to discharge implies active dairy),
--   DVR APU '11' (dairy) and/or category D (dairy farming),
--   LUM subid '502 - Grazed - dairy',
--   pastoral consent records with dairy commodity.
--
-- Ancillary 2.7.3 (effluent land application) is assigned when irrigation records reference
-- effluent or when a dairy effluent discharge consent is present.

CREATE TEMPORARY VIEW class_221 AS (
    SELECT roi.h3_index,
    2 AS lu_code_primary,
    2 AS lu_code_secondary,
    1 AS lu_code_tertiary,
    ROW(
        CASE -- Ancillary: effluent land application
            WHEN (
                irrigation_.ts_notes @@ to_tsquery('english', 'effluent')
                OR dairy_effluent_discharge.h3_index IS NOT NULL
            )
            THEN ARRAY['2.7.3']::TEXT[]
            ELSE ARRAY[]::TEXT[]
        END,
        clamp_confidence_or_null(CASE
            -- Effluent discharge consent with animal count: most direct dairy evidence
            WHEN dairy_effluent_discharge.animal_count > 0
            THEN 1
            -- Effluent discharge confirmed, no count recorded
            WHEN dairy_effluent_discharge.h3_index IS NOT NULL
            THEN 2
            -- DVR dairy APU + category D + LUM dairy subid: three independent confirmations
            WHEN linz_dvr_.actual_property_use = '11'
                AND linz_dvr_.category ~ '^D'
                AND lum_dairy.h3_index IS NOT NULL
            THEN 1
            -- DVR dairy APU with at least one corroborating signal
            WHEN linz_dvr_.actual_property_use = '11'
                AND (
                    linz_dvr_.category ~ '^D'
                    OR lum_dairy.h3_index IS NOT NULL
                    OR pastoral_consents.h3_index IS NOT NULL
                )
            THEN 2
            -- DVR dairy APU alone
            WHEN linz_dvr_.actual_property_use = '11'
            THEN 3
            -- DVR category D alone
            WHEN linz_dvr_.category ~ '^D'
            THEN 6
            -- LUM dairy subid alone
            WHEN lum_dairy.h3_index IS NOT NULL
            THEN 6
            -- Pastoral consent specifying dairy commodity
            WHEN pastoral_consents.h3_index IS NOT NULL
            THEN 6
            ELSE NULL
        END
        + CASE -- LCDB land cover contradicts pastoral/dairy use
            WHEN lcdb_.Class_2023 IN (
                0, 5, 6,
                10, 12, 14, 15, 16,
                20, 21, 22,
                43, 45, 47,
                50, 51, 52, 54, 55,
                64, 68, 69,
                70, 61,
                80, 81
            ) THEN 4
            ELSE 0
        END
        + CASE -- Drip/micro irrigation is inconsistent with pastoral dairy (more likely horticulture)
            WHEN irrigation_.irrigation_type LIKE 'Drip%' THEN 3
            ELSE 0
        END
        + CASE -- Very low capability land (LUC 0 or 8) unlikely for dairy; LUC 7 minor penalty
            WHEN nzlri_lowcapability.lcorrclass = 0 THEN NULL -- Impossible
            WHEN nzlri_lowcapability.lcorrclass = 8 THEN 8
            WHEN nzlri_lowcapability.lcorrclass = 7 THEN 3
            ELSE 0
        END
        + CASE -- Winter forage weakly corroborates dairy support land use
            WHEN winter_forage_.CertRating = 3
                OR (winter_forage_.CertRating = 1 AND winter_forage_.CR1Case = 2)
            THEN -1 -- Good/heavily grazed forage crop: consistent with dairy support
            ELSE 0
        END
        + CASE -- Residential zoning contradicts commercial pastoral/dairy use
            WHEN linz_dvr_."zone" ~ '^9' THEN 3
            ELSE 0
        END
        + CASE -- Pastoral consent with dairy commodity corroborates across all cases
            WHEN pastoral_consents.h3_index IS NOT NULL THEN -2
            ELSE 0
        END),
        ARRAY['cattle dairy']::TEXT[], -- commod: dairy by definition
        ARRAY_REMOVE(ARRAY[irrigation_.manage], NULL)::TEXT[]
        || COALESCE(dairy_effluent_discharge.manage, ARRAY[]::TEXT[]), -- manage
        ARRAY[
            linz_dvr_.source_data,
            lcdb_.source_data,
            lum_dairy.source_data,
            irrigation_.source_data,
            dairy_effluent_discharge.source_data,
            pastoral_consents.source_data,
            winter_forage_.source_data,
            nzlri_lowcapability.source_data
        ]::TEXT[],
        range_merge(datemultirange(VARIADIC ARRAY_REMOVE(ARRAY[
            linz_dvr_.source_date,
            lcdb_.source_date,
            lum_dairy.source_date,
            irrigation_.source_date,
            dairy_effluent_discharge.source_date,
            pastoral_consents.source_date,
            winter_forage_.source_date,
            nzlri_lowcapability.source_date
        ], NULL)))::daterange,
        range_merge(int4multirange(VARIADIC ARRAY_REMOVE(ARRAY[
            linz_dvr_.source_scale,
            lcdb_.source_scale,
            lum_dairy.source_scale,
            irrigation_.source_scale,
            dairy_effluent_discharge.source_scale,
            pastoral_consents.source_scale,
            winter_forage_.source_scale,
            nzlri_lowcapability.source_scale
        ], NULL)))::int4range,
        NULL
    )::nzlum_type AS nzlum_type
    FROM roi
    LEFT JOIN (
        SELECT
            h3_index, source_data, source_date, source_scale,
            actual_property_use, category, "zone"
        FROM linz_dvr_
        WHERE actual_property_use = '11'  -- Dairy
           OR actual_property_use = '01'  -- Multi-use at primary level, rural-industry
           OR category ~ '^D'             -- Dairy category
    ) linz_dvr_ ON roi.h3_index && linz_dvr_.h3_index
    LEFT JOIN (
        -- LCDB classes that would contradict pastoral/dairy use
        SELECT h3_index, source_data, source_date, source_scale, Class_2023
        FROM lcdb_
        WHERE Class_2023 IN (
            0, 5, 6,
            10, 12, 14, 15, 16,
            20, 21, 22,
            43, 45, 47,
            50, 51, 52, 54, 55,
            64, 68, 69,
            70, 61,
            80, 81
        )
    ) AS lcdb_ ON roi.h3_index && lcdb_.h3_index
    LEFT JOIN (
        -- LUM records specifically showing dairy grazing
        SELECT h3_index, source_data, source_date, source_scale
        FROM lum_
        WHERE subid_2020 = '502 - Grazed - dairy'
    ) lum_dairy ON roi.h3_index && lum_dairy.h3_index
    LEFT JOIN (
        SELECT
            h3_index, source_data, source_date, source_scale,
            manage, "status", irrigation_type, ts_notes
        FROM irrigation_
        WHERE (
            ts_notes IS NULL
            OR NOT ts_notes @@ to_tsquery('english', 'sports & field | golf & course')
        )
    ) irrigation_ ON roi.h3_index && irrigation_.h3_index
    LEFT JOIN (
        SELECT h3_index, source_data, source_date, source_scale, manage, animal_count
        FROM dairy_effluent_discharge
    ) AS dairy_effluent_discharge ON roi.h3_index && dairy_effluent_discharge.h3_index
    LEFT JOIN (
        -- Pastoral farm consents specifying dairy commodity
        SELECT h3_index, source_data, source_date, source_scale
        FROM pastoral_consents
        WHERE commod && ARRAY['cattle dairy']::TEXT[]
    ) AS pastoral_consents ON roi.h3_index && pastoral_consents.h3_index
    LEFT JOIN (
        SELECT h3_index, source_data, source_date, source_scale, CertRating, CR1Case
        FROM winter_forage_
    ) AS winter_forage_ ON roi.h3_index && winter_forage_.h3_index
    LEFT JOIN (
        SELECT h3_index, lcorrclass, source_data, source_date, source_scale
        FROM nzlri_luc
        JOIN nzlri_luc_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND lcorrclass IN (0, 7, 8)
    ) AS nzlri_lowcapability ON roi.h3_index && nzlri_lowcapability.h3_index
    WHERE dairy_effluent_discharge.h3_index IS NOT NULL
       OR linz_dvr_.h3_index IS NOT NULL
       OR lum_dairy.h3_index IS NOT NULL
       OR pastoral_consents.h3_index IS NOT NULL
);
