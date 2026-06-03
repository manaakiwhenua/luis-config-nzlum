-- 2.9.0 Smallholder farm
-- Properties that exceed the lifestyle block size threshold (> 2 ha) but still show
-- residential/lifestyle character in the DVR. These are too large for 3.1.4 Rural residential
-- (which covers 0.4–2 ha) but lack evidence of commercial pastoral production that would
-- place them in 2.2.x.
--
-- The defining signal is the combination of over_sized_for_lifestyle (> 2 ha, derived in
-- linz_dvr_ from unit_of_property.land_area) with lifestyle DVR codes (APU 21/22 or category L).
-- These properties are often "gentleman farms" or hobby farms where the primary motivation
-- is residential rather than agricultural production.
--
-- Category notes:
--   LB = lifestyle - bare or substantially unimproved land (subdivision potential)
--   LI = lifestyle - improved with some residential accommodation
--   LV = lifestyle - vacant/substantially unimproved without immediate subdivision potential
--
-- Primary evidence: over_sized_for_lifestyle + APU 21 (lifestyle single-unit),
--   APU 22 (lifestyle multi-unit), category LI (lifestyle improved).
-- Contradictions (→ NULL): high-density indicators, industrial zoning,
--   winter forage (commercial-scale pastoral management), land area > 40 ha.

CREATE TEMPORARY VIEW class_290 AS (
    SELECT roi.h3_index,
    2 AS lu_code_primary,
    9 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN linz_dvr_.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            clamp_confidence_or_null(CASE
                -- Lifestyle single-unit (APU 21) with dwelling in over-sized property: strong signal
                WHEN linz_dvr_.actual_property_use = '21'
                    AND linz_dvr_.improvements_description ~ '\mDWG\M'
                    AND linz_dvr_.over_sized_for_lifestyle
                THEN 2
                -- Lifestyle multi-unit (APU 22) in over-sized property
                WHEN linz_dvr_.actual_property_use = '22'
                    AND linz_dvr_.over_sized_for_lifestyle
                THEN 3
                -- Lifestyle category (LB bare, LI improved) with dwelling in over-sized property
                WHEN linz_dvr_.category ~ '^L(B|I)'
                    AND linz_dvr_.improvements_description ~ '\mDWG\M'
                    AND linz_dvr_.over_sized_for_lifestyle
                THEN 3
                -- APU 21 without confirmed dwelling, over-sized
                WHEN linz_dvr_.actual_property_use = '21'
                    AND linz_dvr_.over_sized_for_lifestyle
                THEN 4
                -- Single residential unit (APU 91) in rural context, over-sized: likely a farmhouse
                WHEN linz_dvr_.actual_property_use = '91'
                    AND rural.h3_index IS NOT NULL
                    AND linz_dvr_.over_sized_for_lifestyle
                THEN 5
                -- Lifestyle category without dwelling (LB bare, LI improved, LV vacant), over-sized
                WHEN linz_dvr_.category ~ '^L(B|I|V)'
                    AND linz_dvr_.over_sized_for_lifestyle
                THEN 6
                -- Lifestyle vacant (APU 29), over-sized: possible smallholder in transition
                WHEN linz_dvr_.actual_property_use = '29'
                    AND linz_dvr_.over_sized_for_lifestyle
                THEN 8
                ELSE NULL
            END
            + CASE -- High/medium density indicators contradict smallholder character
                WHEN linz_dvr_.improvements_description ~ '\mAPARTMENT\M'
                    OR linz_dvr_.improvements_description ~ '\m(THSE|TCE\s?HSE)\M'
                    OR linz_dvr_.improvements_description ~* '\mTOWN\s?HOUSE\M'
                    OR (regexp_match(linz_dvr_.improvements_description, '\m(\d+)\s+(UNIT|FLAT)\M'))[1]::int >= 3
                THEN 99
                ELSE 0
            END
            + CASE -- Industrial zoning contradicts smallholder/residential use
                WHEN linz_dvr_."zone" ~ '^7' THEN 99
                ELSE 0
            END
            + CASE -- Zone modifier: rural confirms smallholder character; residential contradicts it
                WHEN linz_dvr_."zone" ~ '^9' THEN 2  -- Residential zone: urban-fringe, not smallholder
                WHEN linz_dvr_."zone" ~ '^1' THEN -1 -- Rural zone: confirms rural/agricultural setting
                ELSE 0
            END
            + CASE -- Winter forage cultivation contradicts smallholder character (requires commercial-scale management)
                WHEN winter_forage_.h3_index IS NOT NULL THEN 99
                ELSE 0
            END
            + CASE -- Graduated penalty for large properties: smallholder character diminishes with size
                WHEN linz_dvr_.land_area > 40 THEN 99  -- Too large for a lifestyle block → NULL
                WHEN linz_dvr_.land_area > 20 THEN 4   -- Very large: questionable smallholder character
                WHEN linz_dvr_.land_area > 10 THEN 2   -- Large: some reduction in confidence
                ELSE 0
            END
            ),
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale,
            NULL
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
            over_sized_for_lifestyle,
            "zone"
        FROM linz_dvr_
        WHERE over_sized_for_lifestyle = TRUE
          AND (
              actual_property_use IN ('21', '22', '29') -- lifestyle codes
              OR actual_property_use = '91'             -- residential single-unit (possible rural farmhouse)
              OR actual_property_use ~ '^0[29]'         -- multi-use with lifestyle or residential primary
              OR category ~ '^L(B|I|V)'                 -- lifestyle categories
          )
    ) linz_dvr_ ON roi.h3_index && linz_dvr_.h3_index
    LEFT JOIN (
        SELECT h3_index
        FROM winter_forage_
    ) AS winter_forage_ ON roi.h3_index && winter_forage_.h3_index
    LEFT JOIN (
        -- Rural context: used to confirm smallholder character for APU 91 properties
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
