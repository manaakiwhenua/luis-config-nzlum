-- 3.1.4 Rural residential
-- Residential properties with low-intensity (non-commercial) land management practices
-- on land in rural or peri-urban areas.
-- Concordant with the 'Rural lifestyle zone' from the Zone Framework Standard.
--
-- As a general guideline, properties with a dwelling and between 0.4 ha and 2 ha in size
-- should be considered rural residential. Larger properties (> 2 ha) belong under class 2
-- (rural production/extensive land use), even if declared residential or lifestyle in the DVR.
-- Properties in the 0.4–2 ha interval without a dwelling could be in transition to residential.
--
-- Primary evidence: APU 21 (lifestyle single-unit), 22 (lifestyle multi-unit), 97 (bach in rural area)
--   DVR category L (lifestyle: LB, LI, LV), sized_for_lifestyle flag
-- Contradictions (→ NULL): over_sized_for_lifestyle (> 2 ha → class 2.x),
--   very small land area in urban context, high-density indicators, industrial zoning
CREATE TEMPORARY VIEW class_314 AS (
    SELECT roi.h3_index,
    3 AS lu_code_primary,
    1 AS lu_code_secondary,
    4 AS lu_code_tertiary,
    CASE
        WHEN linz_dvr_.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            clamp_confidence_or_null(CASE
                -- Strongest: lifestyle single-unit (APU 21) with dwelling, confirmed in 0.4–2 ha range
                WHEN linz_dvr_.actual_property_use = '21'
                    AND linz_dvr_.improvements_description ~ '\mDWG\M'
                    AND linz_dvr_.sized_for_lifestyle
                THEN 2
                -- Lifestyle multi-unit (APU 22) in 0.4–2 ha range
                WHEN linz_dvr_.actual_property_use = '22'
                    AND linz_dvr_.sized_for_lifestyle
                THEN 3
                -- Bach (APU 97) in lifestyle-size range — rural holiday properties
                WHEN linz_dvr_.actual_property_use = '97'
                    AND linz_dvr_.sized_for_lifestyle
                THEN 3
                -- Category L (lifestyle) with dwelling indicator
                WHEN linz_dvr_.category ~ '^L(B|I)'
                    AND linz_dvr_.improvements_description ~ '\mDWG\M'
                THEN 3
                -- APU 21 without confirmed dwelling, in 0.4–2 ha range
                WHEN linz_dvr_.actual_property_use = '21'
                    AND linz_dvr_.sized_for_lifestyle
                THEN 4
                -- APU 91 (single residential unit) in rural context (rural settlement or rural other)
                WHEN linz_dvr_.actual_property_use = '91'
                    AND rural.h3_index IS NOT NULL
                    AND linz_dvr_.sized_for_lifestyle
                THEN 4
                -- Category L with no dwelling (transitional, possibly pre-residential)
                WHEN linz_dvr_.category ~ '^L(B|I|V)'
                    AND linz_dvr_.sized_for_lifestyle
                THEN 6
                -- APU 29 (lifestyle vacant) in lifestyle-size range — could be transitional
                WHEN linz_dvr_.actual_property_use = '29'
                    AND linz_dvr_.sized_for_lifestyle
                THEN 8
                ELSE NULL
            END
            +
            CASE -- Penalty: oversized (> 2 ha) → class 2.x, not rural residential
                WHEN linz_dvr_.over_sized_for_lifestyle THEN 99
                ELSE 0
            END
            +
            CASE -- Penalty: high/medium density indicators contradict rural residential
                WHEN linz_dvr_.improvements_description ~ '\mAPARTMENT\M'
                    OR linz_dvr_.improvements_description ~ '\m(THSE|TCE\s?HSE)\M'
                    OR linz_dvr_.improvements_description ~* '\mTOWN\s?HOUSE\M'
                    OR (regexp_match(linz_dvr_.improvements_description, '\m(\d+)\s+(UNIT|FLAT)\M'))[1]::int >= 3
                THEN 99
                ELSE 0
            END
            +
            CASE -- Penalty: very small urban property contradicts rural character
                WHEN linz_dvr_.land_area IS NOT NULL
                    AND linz_dvr_.land_area < 0.1
                    AND rural.h3_index IS NULL -- not a rural area
                THEN 99
                ELSE 0
            END
            +
            CASE -- Penalty: industrial zoning
                WHEN linz_dvr_."zone" ~ '^7' THEN 99
                ELSE 0
            END
            +
            CASE -- Residential zoning confirms rural-residential character
                WHEN linz_dvr_."zone" ~ '^9' THEN -1
                ELSE 0
            END
            ),
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale,
            NULL -- comment
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM roi
    LEFT JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale,
            actual_property_use,
            category,
            improvements_description,
            land_area,
            sized_for_lifestyle,
            over_sized_for_lifestyle,
            "zone"
        FROM linz_dvr_
        WHERE (
            actual_property_use IN ('21', '22', '29') -- lifestyle use codes
            OR actual_property_use = '97'             -- bach
            OR actual_property_use ~ '^0[29]'         -- multi-use with lifestyle or residential major use
            OR category ~ '^L(B|I|V)'                 -- lifestyle categories
            -- Include single residential unit (APU 91) only if in lifestyle-size range,
            -- as it may be a rural house miscoded as residential rather than lifestyle
            OR (actual_property_use = '91' AND sized_for_lifestyle)
        )
        AND NOT over_sized_for_lifestyle -- > 2 ha → class 2.x
    ) linz_dvr_ ON roi.h3_index && linz_dvr_.h3_index
    LEFT JOIN (
        -- Rural context: rural settlement (21) and rural other (22) from urban-rural classification.
        -- Used to confirm rural character for APU 91 properties coded as residential.
        SELECT h3_index
        FROM urban_rural_current
        JOIN urban_rural_current_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND urban_rural_current.IUR2026_V1_00 IN (
            '21', -- Rural settlement
            '22'  -- Rural other
        )
    ) AS rural ON roi.h3_index && rural.h3_index
    WHERE linz_dvr_.h3_index IS NOT NULL
);
