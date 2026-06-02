-- 3.1.3 Low-density residential
-- Residential properties within urban boundaries that fall within large-lot or low-density
-- residential zones, often single-family, one- to two-storey houses with yards and
-- landscaping, and lower population density.
--
-- Primary evidence: APU 91 (single unit), 97 (bach), topo50 residential areas,
--   DVR category R (residential subcodes A/C/D/F/H/N/R)
-- Improvements description indicators: DWG (dwelling), POOL, SLEEP OUT, STUDIO, DBLEGGE/DBL GGE,
--   BACH, COTTAGE
-- HNZ (Kāinga Ora) properties: fallback for standalone state houses (lower confidence than 312)
-- Contradictions (→ NULL): high/medium density indicators, oversized properties (→ 314),
--   industrial zoning
CREATE TEMPORARY VIEW class_313 AS (
    SELECT roi.h3_index,
    3 AS lu_code_primary,
    1 AS lu_code_secondary,
    3 AS lu_code_tertiary,
    CASE
        WHEN linz_dvr_.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            clamp_confidence_or_null(CASE
                -- APU 91 (single residential unit) with topo50 residential area confirmation
                WHEN linz_dvr_.actual_property_use = '91'
                    AND topo50_res.h3_index IS NOT NULL
                    AND linz_dvr_.improvements_description ~ '\mDWG\M'
                THEN 2
                -- APU 91 with dwelling indicator
                WHEN linz_dvr_.actual_property_use = '91'
                    AND linz_dvr_.improvements_description ~ '\mDWG\M'
                THEN 3
                -- APU 91 with lifestyle-amenity indicators (pool, sleep out, large garage)
                WHEN linz_dvr_.actual_property_use = '91'
                    AND (
                        linz_dvr_.improvements_description ~ '\mPOOL\M'
                        OR linz_dvr_.improvements_description ~ '\mSLEEP\s?OUT\M'
                        OR linz_dvr_.improvements_description ~ '\mSTUDIO\M'
                        OR linz_dvr_.improvements_description ~ '\m(DBLGGE|DBL\s?GGE|DBLEGGE)\M'
                        OR linz_dvr_.mass_other_improvements = 'Y'
                    )
                THEN 3
                -- APU 97 (bach): holiday home, typically low-density
                WHEN linz_dvr_.actual_property_use = '97'
                THEN 4
                -- Bach or cottage keyword without APU confirmation
                WHEN linz_dvr_.improvements_description ~ '\mBACH\M'
                    OR linz_dvr_.improvements_description ~ '\mCOTTAGE\M'
                THEN 4
                -- Dwelling with pool or sleep-out (without APU 91 confirmed)
                WHEN linz_dvr_.improvements_description ~ '\mDWG\M'
                    AND (
                        linz_dvr_.improvements_description ~ '\mPOOL\M'
                        OR linz_dvr_.improvements_description ~ '\mSLEEP\s?OUT\M'
                        OR linz_dvr_.mass_other_improvements = 'Y'
                    )
                THEN 4
                -- APU 91 alone, no further indicators
                WHEN linz_dvr_.actual_property_use = '91'
                THEN 5
                -- Dwelling alone (DWG without APU 91)
                WHEN linz_dvr_.improvements_description ~ '\mDWG\M'
                THEN 5
                -- DVR category residential subtypes that imply standalone property
                WHEN linz_dvr_.category ~ '^R(A|D|F|H|N|R)'   -- RC = converted dwelling with 2+ rental units → 312
                THEN 7
                -- APU 22 (lifestyle multi-unit) on a small property: too small for rural residential (< 0.4 ha),
                -- so it falls here rather than 314
                WHEN linz_dvr_.actual_property_use = '22'
                    AND linz_dvr_.land_area IS NOT NULL
                    AND linz_dvr_.land_area < 0.4
                THEN 8
                ELSE NULL
            END
            +
            CASE -- Penalty: sized for lifestyle (0.4–2 ha) is ambiguous between 313 and 314;
                 -- add modest penalty so 314 can win when rural context is present
                WHEN linz_dvr_.sized_for_lifestyle THEN 2
                ELSE 0
            END
            +
            CASE -- Penalty: oversized → class 2.x or 314
                WHEN linz_dvr_.over_sized_for_lifestyle THEN 99
                ELSE 0
            END
            +
            CASE -- Penalty: high/medium density indicators → let 311/312 win
                WHEN linz_dvr_.improvements_description ~ '\mAPARTMENT\M'
                    OR linz_dvr_.improvements_description ~ '\mPT\s(FLR|BLDG)\M'
                    OR linz_dvr_.improvements_description ~ '\m(THSE|TCE\s?HSE)\M'
                    OR linz_dvr_.improvements_description ~* '\mTOWN\s?HOUSE\M'
                    OR (regexp_match(linz_dvr_.improvements_description, '\m(\d+)\s+(UNIT|FLAT)\M'))[1]::int >= 2
                THEN 99
                ELSE 0
            END
            +
            CASE -- Penalty: industrial zoning
                WHEN linz_dvr_."zone" ~ '^7' THEN 99
                ELSE 0
            END
            +
            CASE -- Penalty: large improvement to land value ratio contradicts lower-density
                 -- (very expensive improvements on small land → more likely multi-unit)
                WHEN linz_dvr_.building_total_floor_area IS NOT NULL
                    AND linz_dvr_.land_area IS NOT NULL
                    AND linz_dvr_.land_area > 0
                    AND (linz_dvr_.building_total_floor_area / (linz_dvr_.land_area * 10000)) > 1.5
                THEN 3 -- floor area ratio > 1.5 suggests intensive development
                ELSE 0
            END
            +
            CASE -- Penalty: multiple distinct properties per H3 cell contradicts low-density
                WHEN property_density_.overlapping_properties >= 3 THEN 3
                WHEN property_density_.overlapping_properties >= 2 THEN 2
                ELSE 0
            END
            +
            CASE -- Penalty: very small lot without any low-density lifestyle indicators
                WHEN linz_dvr_.land_area IS NOT NULL
                    AND linz_dvr_.land_area < 0.03  -- <300 m²
                    AND linz_dvr_.improvements_description !~ '\m(POOL|SLEEP\s?OUT|STUDIO|BACH|COTTAGE|DBLGGE|DBL\s?GGE)\M'
                    AND linz_dvr_.mass_other_improvements IS DISTINCT FROM 'Y'
                THEN 2
                ELSE 0
            END
            ),
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_.source_data, topo50_res.source_data, hnz.source_data]::TEXT[],
            range_merge(datemultirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    linz_dvr_.source_date,
                    topo50_res.source_date,
                    hnz.source_date
                ], NULL)
            ))::daterange,
            range_merge(int4multirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    linz_dvr_.source_scale,
                    topo50_res.source_scale,
                    hnz.source_scale
                ], NULL)
            ))::int4range,
            NULL -- comment
        )::nzlum_type
        -- HNZ (Kāinga Ora) standalone state houses: lower confidence than class_312 default
        WHEN hnz.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            7, -- low confidence: HNZ may be standalone house but density unknown
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
            actual_property_use,
            category,
            improvements_description,
            land_area,
            sized_for_lifestyle,
            over_sized_for_lifestyle,
            building_total_floor_area,
            mass_other_improvements,
            "zone"
        FROM linz_dvr_
        WHERE (
            actual_property_use ~ '^9[17]'       -- single unit residential, bach
            OR actual_property_use ~ '^0[29]'    -- multi-use with residential or lifestyle major use
            OR (actual_property_use = '22' AND land_area < 0.4) -- lifestyle multi-unit but too small for rural residential → 313
            OR category ~ '^R(A|D|F|H|N|R)'   -- RC = converted dwelling with 2+ rental units → 312   -- residential category subtypes
            -- Include multi-unit APU only if improvements description indicates single/low density
            OR (
                actual_property_use = '92'
                AND (
                    improvements_description ~ '\mDWG\M'
                    OR improvements_description ~ '\mBACH\M'
                    OR improvements_description ~ '\mCOTTAGE\M'
                )
                AND improvements_description !~ '\m(APARTMENT|THSE|FLAT|UNIT)\M'
            )
        )
        AND NOT over_sized_for_lifestyle -- properties > 2 ha belong in class 2.x or 314
    ) linz_dvr_ ON roi.h3_index && linz_dvr_.h3_index
    LEFT JOIN (
        -- Kāinga Ora (Housing New Zealand) state houses; absent from DVR.
        -- Lower confidence than class_312 HNZ entry — standalone houses less certain than blocks.
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale
        FROM linz_crosl_
        WHERE managed_by = 'Housing New Zealand'
    ) AS hnz ON roi.h3_index && hnz.h3_index
    LEFT JOIN (
        SELECT h3_index,
            'LINZ' AS source_data,
            DATERANGE(CURRENT_DATE, CURRENT_DATE, '[]') AS source_date,
            '[50,100]'::int4range AS source_scale
        FROM topo50_residential_areas_h3
        WHERE :parent::h3index = h3_partition
    ) AS topo50_res ON roi.h3_index && topo50_res.h3_index
    LEFT JOIN property_density_ ON roi.h3_index = property_density_.h3_index
    WHERE linz_dvr_.h3_index IS NOT NULL
       OR hnz.h3_index IS NOT NULL
);
