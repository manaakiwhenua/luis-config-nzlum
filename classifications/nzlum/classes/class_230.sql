-- Short-rotation and seasonal cropping
-- Arable cropping (2.3.1)
-- Arable and mixed livestock cropping (2.3.2) [NB this is more common than cropping without animals]
-- Short-rotation horticulture (2.3.3)
-- Seasonal flowers and bulbs, and turf farming (2.3.4)
CREATE TEMPORARY VIEW class_230 AS (
    SELECT h3_index,
    2 AS lu_code_primary,
    3 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    ROW(
        ARRAY[]::TEXT[], -- lu_code_ancillary
        LEAST(GREATEST(CASE
            WHEN (
                lcdb_.h3_index IS NOT NULL
                OR lum_.h3_index IS NOT NULL
                OR seasonal_crops.h3_index IS NOT NULL
                OR winter_forage_.h3_index IS NOT NULL
            ) AND linz_dvr_.improvements_description !~ '\m(ORCHARD|NURSERY)\M'
            AND linz_dvr_.improvements_description !~ '\m(GREEN|SHADE|GLASS)\s?(HOUSE|HSE)\M'
            -- TODO irrigation
            THEN CASE
                WHEN linz_dvr_.actual_property_use = '13' -- Arable farming
                THEN CASE
                    WHEN linz_dvr_.category ~ '^A(I|N)'
                    THEN 1
                    ELSE 5
                    END
                WHEN linz_dvr_.actual_property_use = '01' -- Multi-use (rural industry)
                THEN CASE
                    WHEN linz_dvr_.category ~ '^A(I|N)'
                    THEN 3
                    ELSE 5
                    END
                WHEN linz_dvr_.actual_property_use = '15' -- Market gardens and orchards
                THEN CASE
                    WHEN category ~ '^H(B|F|M|X)(A|B)'
                    THEN 3
                    WHEN category ~ '^H(B|F|M|X)C'
                    THEN 4
                    ELSE 6
                    END
                WHEN linz_dvr_.actual_property_use IN (
                    '10', -- Rural, multi-use
                    '11', -- Dairy
                    '12', -- Stock finishing
                    '14', -- Store livestock
                    '16' -- Specialist livestock
                )
                THEN
                    CASE
                        WHEN seasonal_crops.manage IS NOT NULL 
                        THEN 6
                        WHEN winter_forage_.manage IS NOT NULL
                        THEN 6
                        ELSE 8
                    END
                ELSE 12
                END
            WHEN linz_dvr_.actual_property_use = '13'
            THEN CASE
                WHEN linz_dvr_.category ~ '^A(I|N)' AND (seasonal_crops.manage IS NOT NULL OR winter_forage_.manage IS NOT NULL)
                THEN 1
                WHEN linz_dvr_.category ~ '^A(I|N)' OR (seasonal_crops.manage IS NOT NULL OR winter_forage_.manage IS NOT NULL)
                THEN 2
                ELSE 3
                END
            WHEN linz_dvr_.actual_property_use IN ('01', '15')
            THEN CASE
                WHEN linz_dvr_.category ~ '^A(I|N)(A|B|C)'
                THEN 4
                WHEN linz_dvr_.category ~ '^A(I|N)(D|E|F)?'
                THEN 7
                WHEN linz_dvr_.category ~ '^H(B|F|M|X)(A|B|C)'
                THEN 4
                WHEN linz_dvr_.category ~ '^H(B|F|M|X)D'
                THEN 7
                WHEN linz_dvr_.category ~ '^H(B|F|M|X)(E|F)?'
                THEN 9
                WHEN linz_dvr_.actual_property_use = '15'
                THEN 8
                ELSE 12
                END
            WHEN linz_dvr_.category ~ '^A(I|N)'
            THEN 7
            WHEN linz_dvr_.category ~ '^H(B|F|M|X)(A|B)'
            THEN 7
            WHEN linz_dvr_.category ~ '^H(B|F|M|X)(C)'
            THEN 9
            WHEN (
                lcdb_.h3_index IS NOT NULL
                OR lum_.h3_index IS NOT NULL 
            ) AND linz_dvr_.h3_index IS NULL -- Gap filling
            THEN
                CASE
                    WHEN irrigation_.h3_index IS NOT NULL
                    THEN 10
                    ELSE 11
                END
            ELSE NULL
        END
        +
        CASE -- Boost confidence with seasonal crop evidence
            WHEN (
                seasonal_crops.h3_index IS NOT NULL
                OR winter_forage_.h3_index IS NOT NULL
            )
            THEN -1
            ELSE 0
        END
        , 1), 12), -- confidence
        (
            CASE
                WHEN linz_dvr_.actual_property_use = '11'
                THEN ARRAY['cattle dairy']::TEXT[]
                WHEN linz_dvr_.category ~ '^HF'
                THEN ARRAY['flowers and foliage']::TEXT[]
                ELSE ARRAY[]::TEXT[]
            END
        )
        -- || COALESCE(winter_forage_.commod, ARRAY[]::TEXT[])
        || COALESCE(seasonal_crops.commod, ARRAY[]::TEXT[]), -- commod
        (
            CASE
                WHEN irrigation_.h3_index IS NOT NULL
                THEN ARRAY[irrigation_.manage]::TEXT[]
                WHEN linz_dvr_.category ~ '^AI'
                THEN ARRAY['irrigation']::TEXT[]
                ELSE ARRAY[]::TEXT[]
            END
        )
        || COALESCE(seasonal_crops.manage, ARRAY[]::TEXT[])
        || COALESCE(ARRAY[winter_forage_.manage]::TEXT[], ARRAY[]::TEXT[]), -- manage
        ARRAY[
            linz_dvr_.source_data,
            irrigation_.source_data,
            lcdb_.source_data,
            lum_.source_data,
            seasonal_crops.source_data,
            winter_forage_.source_data
        ]::TEXT[], -- source_data
        range_merge(datemultirange(
            VARIADIC ARRAY_REMOVE(ARRAY[
                linz_dvr_.source_date,
                lcdb_.source_date,
                lum_.source_date,
                irrigation_.source_date,
                seasonal_crops.source_date,
                winter_forage_.source_date
            ], NULL)
        ))::daterange, -- source_date
        range_merge(int4multirange(
            VARIADIC ARRAY_REMOVE(ARRAY[
                linz_dvr_.source_scale,
                lcdb_.source_scale,
                lum_.source_scale,
                irrigation_.source_scale,
                seasonal_crops.source_scale,
                winter_forage_.source_scale
            ], NULL)
        ))::int4range -- source_scale
    )::nzlum_type AS nzlum_type
    -- commodity type?
    FROM (
        SELECT *
        FROM lcdb_
        WHERE class_2018 = '30' -- Short-rotation Cropland
    ) lcdb_
    FULL OUTER JOIN (
        SELECT *
        FROM lum_
        WHERE lucid_2020 = '78 - Cropland - Annual'
    ) lum_ USING (h3_index)
    FULL OUTER JOIN (
        SELECT *
        FROM linz_dvr_
        WHERE actual_property_use IN (
            '10', -- Multi-use within rural industry
            -- '11', -- Dairy
            -- '12', -- Stock finishing
            '13', -- Arable
            -- '14', -- Store livestock
            -- '16', -- Specialist livestock
            '01', -- Mixed, rural
            '15' -- Market gardens and orchards [for flowers]
        )
        OR category ~ '^A(I|N)' -- Arable, AI% vs AN% (irrigated/not)
        OR category ~ '^H(B|F|M|X)(A|B|C|D|E|F)?' -- Berry fruits, flowers, market gardens, other hort/mixed
    ) linz_dvr_ USING (h3_index)
    FULL OUTER JOIN (
        SELECT *
        FROM irrigation_
        WHERE ts_notes IS NULL -- Very important 
        OR NOT (ts_notes @@ to_tsquery('english', 'sports & field | golf & course'))
    ) irrigation_ USING (h3_index)
    FULL OUTER JOIN (
        SELECT *
        FROM crop_maps
        WHERE commod && ARRAY[
            'broccoli',
            'cauliflowers',
            'chamomile',
            'flowers and foliage',
            'turnips forage',
            'lettuces',
            'cabbages',
            'lucerne',
            'maize',
            'sweetcorn',
            'melons',
            'plantain',
            'clover',
            'pumpkins'
        ]
        OR manage && ARRAY[
            'residues baled',
            'crop pasture rotation',
            'grazing rotational'
        ]
    ) AS seasonal_crops USING (h3_index)
    FULL OUTER JOIN winter_forage_ USING (h3_index)
);
-- LCDB and LUM are good
-- DVR helps too; for places in grass that are actually part of an arable rotation, LCDB will not have captured, so DVR actual use is key
-- LOWER confidence significantly if the DVR property category is not A%
-- Category A% on its own (i.e. with conflicting actual use) is still positive, esp with LCDB evidence. BUT there have been many dairy conversions, so don't be overconfident
-- Irrigation may provide further evidence, and is also a management practice (available from DVR and from bespoke irrigation layer)
-- Consider also MWLR winter forage mapping, as in s4.5.4 of Southland LU map report

-- NB seems to be very little North Island maize indicated in any of these sources
-- TODO commod_ancillary using consents and cattle numbers?