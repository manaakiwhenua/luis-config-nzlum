-- 3.1.2 Medium-density residential
-- Areas featuring a moderate concentration of housing units per unit of land area,
-- often in suburban or semi-urban settings.
-- Example: townhouses, terraced housing, low-rise apartments.
--
-- Primary evidence: APU 92 (multi-unit without high-density indicators),
--   95 (special accommodation), 96 (communal), 22 (lifestyle multi-unit)
-- NB: APU 93 (motels, holiday parks) and 94 (hotels) → class 3.3.0 Commercial, not residential
-- Improvements description indicators: TOWN HOUSE/THSE, TCE HSE, FLAT, REST HOME
-- HNZ (Kāinga Ora) properties are not in DVR; linz_crosl_ used as a fallback signal
-- Contradictions (→ NULL): high-density indicators (→ 311), oversized properties, industrial zoning
CREATE TEMPORARY VIEW class_312 AS (
    SELECT roi.h3_index,
    3 AS lu_code_primary,
    1 AS lu_code_secondary,
    2 AS lu_code_tertiary,
    CASE
        WHEN linz_dvr_.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            clamp_confidence_or_null(CASE
                -- Townhouse / terrace house indicators: most reliable for medium-density
                WHEN linz_dvr_.improvements_description ~ '\mTHSE\M'
                    OR linz_dvr_.improvements_description ~* '\mTOWN\s?HOUSE\M'
                    OR linz_dvr_.improvements_description ~ '\mTCE\s?HSE\M'
                THEN 2
                -- Flat without a large count (large counts → 311)
                WHEN linz_dvr_.improvements_description ~ '\mFLAT\M'
                    AND (regexp_match(linz_dvr_.improvements_description, '\m(\d+)\s+FLAT\M'))[1]::int < 5
                THEN 3
                WHEN linz_dvr_.improvements_description ~ '\mFLAT\M'
                    AND (regexp_match(linz_dvr_.improvements_description, '\m(\d+)\s+FLAT\M'))[1] IS NULL -- 'FLAT' without count
                THEN 3
                -- Rest home / retirement / special accommodation
                WHEN linz_dvr_.improvements_description ~ '\mREST\s?(HOME|HSE|HOM)\M'
                THEN 3
                WHEN linz_dvr_.actual_property_use = '95' -- special accommodation
                THEN 3
                -- 2–4 units: medium density
                WHEN (regexp_match(linz_dvr_.improvements_description, '\m(\d+)\s+UNIT\M'))[1]::int BETWEEN 2 AND 4
                THEN 3
                WHEN (regexp_match(linz_dvr_.improvements_description, '\m(\d+)\s+DWG\M'))[1]::int BETWEEN 2 AND 4
                THEN 4
                -- Category RR: purpose-built rental flats, 2+ units of use
                -- Best use concordant with actual use → strong; best use alone → moderate
                WHEN linz_dvr_.category ~ '^RR'
                    AND linz_dvr_.actual_property_use IN ('92', '95', '96')
                THEN 3 -- best use matches actual use: good evidence
                WHEN linz_dvr_.category ~ '^RR'
                THEN 5 -- best use only, actual use may differ
                -- Category RC: converted dwelling now used as 2+ rental flats/units
                WHEN linz_dvr_.category ~ '^RC'
                THEN 5
                -- APU 96: communal residential dependent on other use (convent, presbytery)
                WHEN linz_dvr_.actual_property_use = '96'
                THEN 5
                -- APU 92 (multi-unit) without high-density indicators
                WHEN linz_dvr_.actual_property_use = '92'
                THEN 5
                ELSE NULL
            END
            +
            CASE -- Penalty: high-density indicators contradict medium-density → let 311 win
                WHEN linz_dvr_.improvements_description ~ '\mAPARTMENT\M'
                    OR linz_dvr_.improvements_description ~ '\mPT\s(FLR|BLDG)\M'
                    OR (regexp_match(linz_dvr_.improvements_description, '\m(\d+)\s+UNIT\M'))[1]::int >= 5
                    OR (regexp_match(linz_dvr_.improvements_description, '\m(\d+)\s+FLAT\M'))[1]::int >= 5
                THEN 99
                ELSE 0
            END
            +
            CASE -- Penalty: industrial zoning
                WHEN linz_dvr_."zone" ~ '^7' THEN 99
                ELSE 0
            END
            +
            CASE -- Penalty: oversized for lifestyle → not medium density
                WHEN linz_dvr_.over_sized_for_lifestyle THEN 99
                ELSE 0
            END
            +
            CASE -- Boost: multiple distinct properties per H3 cell → spatially dense
                WHEN property_density_.overlapping_properties >= 3 THEN -2
                WHEN property_density_.overlapping_properties >= 2 THEN -1
                ELSE 0
            END
            +
            CASE -- Boost: small lot (≤400 m²) with multi-unit APU → likely medium density
                WHEN linz_dvr_.land_area IS NOT NULL
                    AND linz_dvr_.land_area <= 0.04
                    AND linz_dvr_.actual_property_use = '92'
                THEN -1
                ELSE 0
            END
            +
            CASE -- Boost: high building coverage ratio on small lot → denser development
                WHEN linz_dvr_.building_site_coverage IS NOT NULL
                    AND linz_dvr_.land_area IS NOT NULL
                    AND linz_dvr_.land_area > 0
                    AND (linz_dvr_.building_site_coverage / (linz_dvr_.land_area * 10000)) > 0.4
                    AND linz_dvr_.actual_property_use = '92'
                THEN -1
                ELSE 0
            END
            ),
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_.source_data, hnz.source_data]::TEXT[],
            range_merge(datemultirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    linz_dvr_.source_date,
                    hnz.source_date
                ], NULL)
            ))::daterange,
            range_merge(int4multirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    linz_dvr_.source_scale,
                    hnz.source_scale
                ], NULL)
            ))::int4range,
            NULL -- comment
        )::nzlum_type
        -- HNZ (Kāinga Ora) properties are not in DVR; use as a medium-density fallback
        WHEN hnz.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            5, -- moderate confidence: HNZ housing varies in density but is typically medium
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[hnz.source_data]::TEXT[],
            hnz.source_date,
            hnz.source_scale,
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
            category,
            actual_property_use,
            improvements_description,
            land_area,
            building_site_coverage,
            over_sized_for_lifestyle,
            "zone"
        FROM linz_dvr_
        WHERE (
            actual_property_use IN ('92', '95', '96') -- multi-unit and communal residential (93 = motels/holiday parks, 94 = hotels → 3.3.0; 22 = lifestyle multi-unit → 314)
            OR category ~ '^R(C|R)'               -- RC: converted dwelling 2+ units; RR: purpose-built rental flats
            OR actual_property_use ~ '^0[29]'   -- multi-use with residential or lifestyle major use
            OR actual_property_use = '20'        -- lifestyle multi-use
            -- Include single-unit APU only if improvements description suggests medium density
            OR (
                actual_property_use ~ '^9[17]'
                AND (
                    improvements_description ~ '\m(THSE|TCE\s?HSE)\M'
                    OR improvements_description ~* '\mTOWN\s?HOUSE\M'
                    OR improvements_description ~ '\mFLAT\M'
                    OR improvements_description ~ '\mREST\s?(HOME|HSE)\M'
                )
            )
        )
    ) linz_dvr_ ON roi.h3_index && linz_dvr_.h3_index
    LEFT JOIN (
        -- Kāinga Ora (Housing New Zealand) properties are state-owned and absent from DVR.
        -- Retained as a medium/low density signal; see also class_313.
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale
        FROM linz_crosl_
        WHERE managed_by = 'Housing New Zealand'
    ) AS hnz ON roi.h3_index && hnz.h3_index
    LEFT JOIN property_density_ ON roi.h3_index = property_density_.h3_index
    WHERE linz_dvr_.h3_index IS NOT NULL
       OR hnz.h3_index IS NOT NULL
);
