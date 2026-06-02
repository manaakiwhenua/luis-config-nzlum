-- 3.1.1 High-density residential
-- Areas characterised by a high concentration of housing units per unit of land area,
-- typically in multi-storey buildings or high-rise developments, often found in urban
-- centres supporting high population density.
--
-- Primary evidence: APU 92 (multi-unit), 93/95/96; category RA (best-use: strata apartments — weak)
-- Improvements description indicators: APARTMENT, PT FLR, PT BLDG, large unit counts,
--   BRD HOUSE, RES ACCOM, UNIT/FLAT multipliers
-- Contradictions (→ NULL): over-sized properties, industrial zoning, single-unit indicators
CREATE TEMPORARY VIEW class_311 AS (
    SELECT roi.h3_index,
    3 AS lu_code_primary,
    1 AS lu_code_secondary,
    1 AS lu_code_tertiary,
    CASE
        WHEN linz_dvr_.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            clamp_confidence_or_null(CASE
                -- Strongest evidence: building type clearly indicates stacked/shared structure
                WHEN linz_dvr_.improvements_description ~ '\mAPARTMENT\M'
                THEN 2
                WHEN linz_dvr_.improvements_description ~ '\mPT\sFLR\M' -- part floor
                    OR linz_dvr_.improvements_description ~ '\mPT\sBLDG\M' -- part building
                THEN 2
                WHEN linz_dvr_.improvements_description ~ '\mBRD\s(HOUSE|HSE)\M' -- boarding house
                    OR linz_dvr_.improvements_description ~ '\mRES\sACCOM\M' -- residential accommodation
                THEN 3
                -- Large unit or flat counts extracted from improvements description
                WHEN (regexp_match(linz_dvr_.improvements_description, '\m(\d+)\s+UNIT\M'))[1]::int >= 5
                THEN 2
                WHEN (regexp_match(linz_dvr_.improvements_description, '\m(\d+)\s+FLAT\M'))[1]::int >= 5
                THEN 2
                WHEN (regexp_match(linz_dvr_.improvements_description, '\m(\d+)\s+DWG\M'))[1]::int >= 8
                THEN 2 -- Many dwellings on one title → high density
                -- APU 92 (multi-unit) with small land area → densely packed units
                WHEN linz_dvr_.actual_property_use = '92'
                    AND linz_dvr_.land_area IS NOT NULL
                    AND linz_dvr_.land_area < 0.1
                THEN 3
                -- Medium unit/flat counts
                WHEN (regexp_match(linz_dvr_.improvements_description, '\m(\d+)\s+UNIT\M'))[1]::int BETWEEN 2 AND 4
                THEN 4
                WHEN (regexp_match(linz_dvr_.improvements_description, '\m(\d+)\s+FLAT\M'))[1]::int BETWEEN 2 AND 4
                THEN 4
                WHEN (regexp_match(linz_dvr_.improvements_description, '\m(\d+)\s+DWG\M'))[1]::int BETWEEN 3 AND 7
                THEN 5
                -- APU 92 with any unit/flat indicator but count not determined
                WHEN linz_dvr_.actual_property_use = '92'
                    AND (
                        linz_dvr_.improvements_description ~ '\mUNIT\M'
                        OR linz_dvr_.improvements_description ~ '\mFLAT\M'
                    )
                THEN 5
                -- APU 93/95/96: communal or special accommodation
                WHEN linz_dvr_.actual_property_use IN ('93', '95', '96')
                THEN 5
                -- APU 92 alone, no further density indicators
                WHEN linz_dvr_.actual_property_use = '92'
                THEN 7
                -- Any unit/flat indicator without a confirmed APU
                WHEN linz_dvr_.improvements_description ~ '\mUNIT\M'
                    OR linz_dvr_.improvements_description ~ '\mFLAT\M'
                THEN 8
                -- Category RR (purpose-built rental flats, 2+ units of use)
                -- Concordant best+actual use strengthens the signal; best use alone is weaker
                WHEN linz_dvr_.category ~ '^RR'
                    AND linz_dvr_.actual_property_use IN ('92', '95', '96')
                THEN 5 -- concordant
                WHEN linz_dvr_.category ~ '^RR'
                THEN 7 -- best use only
                -- Category RA (best use: strata-title apartments in multi-storey buildings)
                -- Concordant best+actual use strengthens the signal; best use alone is weak
                WHEN linz_dvr_.category ~ '^RA'
                    AND linz_dvr_.actual_property_use IN ('92', '95', '96')
                THEN 6 -- concordant
                WHEN linz_dvr_.category ~ '^RA'
                THEN 9 -- best use only; placed last so stronger signals always take priority
                ELSE NULL -- No high-density evidence
            END
            +
            CASE -- Penalty: industrial zoning strongly contradicts residential
                WHEN linz_dvr_."zone" ~ '^7' THEN 99 -- forces NULL via clamp
                ELSE 0
            END
            +
            CASE -- Penalty: oversized property contradicts high-density
                WHEN linz_dvr_.over_sized_for_lifestyle THEN 99
                ELSE 0
            END
            +
            CASE -- Penalty: single-unit APU contradicts high density
                WHEN linz_dvr_.actual_property_use IN ('91', '97', '21')
                    AND linz_dvr_.improvements_description !~ '\mAPARTMENT\M'
                    AND linz_dvr_.improvements_description !~ '\mPT\s(FLR|BLDG)\M'
                    AND linz_dvr_.improvements_description !~ '\mUNIT\M'
                    AND linz_dvr_.improvements_description !~ '\mFLAT\M'
                THEN 99
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
            over_sized_for_lifestyle,
            "zone"
        FROM linz_dvr_
        WHERE (
            actual_property_use IN ('92', '93', '95', '96') -- multi-unit and communal residential (94 = hotels → 3.3.0)
            OR actual_property_use ~ '^0[29]'   -- multi-use with residential major use
            OR category ~ '^R(A|R)'               -- RA: best-use strata apartments; RR: purpose-built rental flats (2+ units)
            -- Include single-unit APU only if improvements description has density indicators
            OR (
                actual_property_use ~ '^9[17]'
                AND (
                    improvements_description ~ '\mAPARTMENT\M'
                    OR improvements_description ~ '\mPT\s(FLR|BLDG)\M'
                    OR improvements_description ~ '\m\d+\s+(UNIT|FLAT|DWG)\M'
                )
            )
        )
    ) linz_dvr_ ON roi.h3_index && linz_dvr_.h3_index
    WHERE linz_dvr_.h3_index IS NOT NULL
);
