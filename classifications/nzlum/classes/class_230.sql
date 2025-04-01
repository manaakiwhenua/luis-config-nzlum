-- Short-rotation and seasonal cropping
-- Arable cropping (2.3.1)
-- Arable and mixed livestock cropping (2.3.2) [NB this is more common than cropping without animals]
-- Short-rotation horticulture (2.3.3)
-- Seasonal flowers and bulbs, and turf farming (2.3.4)
-- TODO add an area around Ohakune for validation
CREATE TEMPORARY VIEW class_230 AS (
    SELECT h3_index,
    2 AS lu_code_primary,
    3 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    ROW(
        ARRAY[]::TEXT[], -- lu_code_ancillary
        CASE
            WHEN (
                lcdb_.h3_index IS NOT NULL
                OR lum_.h3_index IS NOT NULL
            ) AND linz_dvr_.improvements_description !~ '\m(ORCHARD|NURSERY)\M'
            AND linz_dvr_.improvements_description !~ '\m(GREEN|SHADE|GLASS)\s?(HOUSE|HSE)\M'
            -- TODO irrigation
            THEN CASE
                WHEN linz_dvr_.actual_property_use = '13'
                THEN CASE
                    WHEN linz_dvr_.category ~ '^A(I|N)'
                    THEN 1
                    ELSE 5
                    END
                WHEN linz_dvr_.actual_property_use = '01'
                THEN CASE
                    WHEN linz_dvr_.category ~ '^A(I|N)'
                    THEN 3
                    ELSE 5
                    END
                WHEN linz_dvr_.actual_property_use = '15'
                THEN CASE
                    WHEN category ~ '^H(B|F|M|X)(A|B)'
                    THEN 3
                    WHEN category ~ '^H(B|F|M|X)C'
                    THEN 4
                    ELSE 6
                    END
                ELSE 2
                END
            WHEN linz_dvr_.actual_property_use = '13'
            THEN CASE
                WHEN linz_dvr_.category ~ '^A(I|N)'
                THEN 1
                ELSE 7
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
                    THEN 2
                    ELSE 11
                END
            ELSE NULL
        END, -- confidence
        CASE
            WHEN linz_dvr_.category ~ '^HF'
            THEN ARRAY['flowers and foliage']::TEXT[]
            ELSE ARRAY[]::TEXT[]
        END, -- commod
        CASE
            WHEN irrigation_.h3_index IS NOT NULL
            THEN ARRAY[irrigation_.manage]::TEXT[]
            WHEN linz_dvr_.category ~ '^AI'
            THEN ARRAY['irrigation']::TEXT[]
            ELSE ARRAY[]::TEXT[]
        END, -- manage
        ARRAY[
            linz_dvr_.source_data,
            irrigation_.source_data,
            lcdb_.source_data,
            lum_.source_data
        ]::TEXT[], -- source_data
        range_merge(datemultirange(
            VARIADIC ARRAY_REMOVE(ARRAY[
                linz_dvr_.source_date,
                lcdb_.source_date,
                lum_.source_date,
                irrigation_.source_date
            ], NULL)
        ))::daterange, -- source_date
        range_merge(int4multirange(
            VARIADIC ARRAY_REMOVE(ARRAY[
                linz_dvr_.source_scale,
                lcdb_.source_scale,
                lum_.source_scale,
                irrigation_.source_scale
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
            '13', -- Arable
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
);
-- LCDB and LUM are good
-- DVR helps too; for places in grass that are actually part of an arable rotation, LCDB will not have captured, so DVR actual use is key
-- LOWER confidence significantly if the DVR property category is not A%
-- Category A% on its own (i.e. with conflicting actual use) is still positive, esp with LCDB evidence. BUT there have been many dairy conversions, so don't be overconfident
-- Irrigation may provide further evidence, and is also a management practice (available from DVR and from bespoke irrigation layer)
-- TODO winter forage, as in s4.5.4 of Southland LU map report
-- NB seems to be very little North Island maize indicated in these sources
-- TODO commod_ancillary using consents and cattle numbers