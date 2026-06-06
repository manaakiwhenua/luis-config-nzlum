CREATE TEMPORARY VIEW class_240 AS ( -- Perennial horticulture
    With crop_perennial AS (
        SELECT h3_index,
            source_data,
            source_date,
            source_scale,
            commod
        FROM crop_maps
        WHERE commod && ARRAY[
            'apples',
            'avocados',
            'feijoa',
            'pears',
            'kiwifruit',
            'grapes',
            'olives',
            'persimmons',
            'pinenuts',
            'pomegranate',
            'tamarillo'
        ]::TEXT[]
        OR (
            source_data = 'GDC'
            AND crop && ARRAY[
                'Citrus',
                'Stonefruit'
            ]
        ) -- NB special cases for GDC data that don't map to specific commodities
    ),
    base_classification AS (
        SELECT roi.h3_index,
        2 AS lu_code_primary,
        4 AS lu_code_secondary,
        0 AS lu_code_tertiary,
        CASE
            WHEN (
                lum_.h3_index IS NOT NULL
                AND topo50_orchards_.h3_index IS NOT NULL
            ) OR crop_perennial.h3_index IS NOT NULL
            THEN ROW(
                ARRAY[]::TEXT[], -- lu_code_ancillary
                CASE
                    WHEN linz_dvr_.improvements_description ~ '\mORCHARD\M'
                    THEN 1
                    WHEN linz_dvr_.category ~ '^H(C|K|P|S|V)(A|B|C)'
                    THEN 2
                    WHEN linz_dvr_.category ~ '^H(C|K|P|S|V)(D|E|F)?'
                    THEN 5
                    WHEN linz_dvr_.category ~ '^HX(A|B)'
                    THEN 5
                    WHEN linz_dvr_.category ~ '^H(B|F|M|X)'
                    THEN 9
                    ELSE 4 -- confidence
                END,
                COALESCE(crop_perennial.commod, ARRAY[]::TEXT[]) ||
                ARRAY[CASE
                    WHEN linz_dvr_.category ~ 'HK(A|B)'
                    THEN 'kiwifruit'
                    WHEN linz_dvr_.category ~ 'HV(A|B)'
                    THEN 'grapes'
                    ELSE NULL
                END]::TEXT[], -- commod
                ARRAY[]::TEXT[], -- manage
                ARRAY[lum_.source_data, topo50_orchards_.source_data, linz_dvr_.source_data, crop_perennial.source_data]::TEXT[], -- source_data
                range_merge(datemultirange(
                    VARIADIC ARRAY_REMOVE(ARRAY[
                        topo50_orchards_.source_date,
                        lum_.source_date,
                        linz_dvr_.source_date,
                        crop_perennial.source_date
                    ], NULL)
                ))::daterange, -- source_date
                range_merge(int4multirange(
                    VARIADIC ARRAY_REMOVE(ARRAY[
                        topo50_orchards_.source_scale,
                        lum_.source_scale,
                        linz_dvr_.source_scale,
                        crop_perennial.source_scale
                    ], NULL)
                ))::int4range, -- source_scale
                NULL
            )::nzlum_type
            WHEN (
                lcdb_.h3_index IS NOT NULL
                AND topo50_orchards_.h3_index IS NOT NULL
            )
            THEN ROW(
                ARRAY[]::TEXT[], -- lu_code_ancillary
                CASE
                    WHEN linz_dvr_.improvements_description ~ '\mORCHARD\M'
                    THEN 1
                    WHEN linz_dvr_.category ~ '^H(C|K|P|S|V)(A|B|C)'
                    THEN 2
                    WHEN linz_dvr_.category ~ '^H(C|K|P|S|V)(D|E|F)?'
                    THEN 5
                    WHEN linz_dvr_.category ~ '^HX'
                    THEN 5
                    WHEN linz_dvr_.category ~ '^H(B|F|M)'
                    THEN 9
                    ELSE 4 -- confidence
                END,
                ARRAY[CASE
                    WHEN linz_dvr_.category ~ 'HK(A|B)'
                    THEN 'kiwifruit'
                    WHEN linz_dvr_.category ~ 'HV(A|B)'
                    THEN 'grapes'
                    ELSE NULL
                END]::TEXT[], -- commod
                ARRAY[]::TEXT[], -- manage
                ARRAY[lcdb_.source_data, topo50_orchards_.source_data, linz_dvr_.source_data]::TEXT[], -- source_data
                range_merge(datemultirange(
                    VARIADIC ARRAY_REMOVE(ARRAY[
                        topo50_orchards_.source_date,
                        lcdb_.source_date,
                        linz_dvr_.source_date
                    ], NULL)
                ))::daterange, -- source_date
                range_merge(int4multirange(
                    VARIADIC ARRAY_REMOVE(ARRAY[
                        topo50_orchards_.source_scale,
                        lcdb_.source_scale,
                        linz_dvr_.source_scale
                    ], NULL)
                ))::int4range, -- source_scale
                NULL
            )::nzlum_type
            WHEN topo50_orchards_.h3_index IS NOT NULL
            THEN ROW(
                ARRAY[]::TEXT[], -- lu_code_ancillary
                CASE
                    WHEN linz_dvr_.actual_property_use = '15'
                    THEN 1
                    WHEN linz_dvr_.improvements_description ~ '\mORCHARD\M'
                    THEN 1
                    WHEN linz_dvr_.category ~ '^H(C|K|P|S|V)(A|B|C)'
                    THEN 2
                    WHEN linz_dvr_.category ~ '^H(C|K|P|S|V)(D|E|F)?'
                    THEN 5
                    WHEN linz_dvr_.category ~ '^HX'
                    THEN 5
                    WHEN linz_dvr_.category ~ '^H(B|F|M)'
                    THEN 9
                    ELSE 6 -- confidence
                END,
                ARRAY[CASE
                    WHEN linz_dvr_.category ~ 'HK(A|B)'
                    THEN 'kiwifruit'
                    WHEN linz_dvr_.category ~ 'HV(A|B)'
                    THEN 'grapes'
                    ELSE NULL
                END]::TEXT[], -- commod
                ARRAY[]::TEXT[], -- manage
                ARRAY[topo50_orchards_.source_data, linz_dvr_.source_data]::TEXT[], -- source_data
                range_merge(datemultirange(
                    VARIADIC ARRAY_REMOVE(ARRAY[
                        topo50_orchards_.source_date,
                        linz_dvr_.source_date
                    ], NULL)
                ))::daterange, -- source_date
                range_merge(int4multirange(
                    VARIADIC ARRAY_REMOVE(ARRAY[
                        topo50_orchards_.source_scale,
                        linz_dvr_.source_scale
                    ], NULL)
                ))::int4range, -- source_scale
                NULL
            )::nzlum_type
            WHEN lum_.h3_index IS NOT NULL
            THEN ROW(
                ARRAY[]::TEXT[], -- lu_code_ancillary
                CASE
                    WHEN linz_dvr_.actual_property_use = '15'
                    THEN 1
                    WHEN linz_dvr_.improvements_description ~ '\mORCHARD\M'
                    THEN 2
                    WHEN linz_dvr_.category ~ '^H(C|K|P|S|V)(A|B|C)'
                    THEN 2
                    WHEN linz_dvr_.category ~ '^H(C|K|P|S|V)(D|E|F)?'
                    THEN 5
                    WHEN linz_dvr_.category ~ '^HX'
                    THEN 5
                    WHEN linz_dvr_.category ~ '^H(B|F|M)'
                    THEN 9
                    ELSE 6
                END,  -- confidence
                ARRAY[CASE
                    WHEN linz_dvr_.category ~ 'HK(A|B)'
                    THEN 'kiwifruit'
                    WHEN linz_dvr_.category ~ 'HV(A|B)'
                    THEN 'grapes'
                    ELSE NULL
                END]::TEXT[], -- commod
                ARRAY[]::TEXT[], -- manage
                ARRAY[lum_.source_data, linz_dvr_.source_data]::TEXT[], -- source_data
                range_merge(datemultirange(
                    VARIADIC ARRAY_REMOVE(ARRAY[
                        lum_.source_date,
                        linz_dvr_.source_date
                    ], NULL)
                ))::daterange, -- source_date
                range_merge(int4multirange(
                    VARIADIC ARRAY_REMOVE(ARRAY[
                        lum_.source_scale,
                        linz_dvr_.source_scale
                    ], NULL)
                ))::int4range,
                NULL
            )::nzlum_type
            WHEN lcdb_.h3_index IS NOT NULL
            THEN ROW(
                ARRAY[]::TEXT[], -- lu_code_ancillary
                CASE
                    WHEN linz_dvr_.actual_property_use = '15'
                    THEN 1
                    WHEN linz_dvr_.improvements_description ~ '\mORCHARD\M'
                    THEN 2
                    WHEN linz_dvr_.category ~ '^H(C|K|P|S|V)(A|B|C)'
                    THEN 2
                    WHEN linz_dvr_.category ~ '^H(C|K|P|S|V)(D|E|F)?'
                    THEN 5
                    WHEN linz_dvr_.category ~ '^HX'
                    THEN 5
                    WHEN linz_dvr_.category ~ '^H(B|F|M)'
                    THEN 9
                    ELSE 6
                END,  -- confidence
                ARRAY[CASE
                    WHEN linz_dvr_.category ~ 'HK(A|B)'
                    THEN 'kiwifruit'
                    WHEN linz_dvr_.category ~ 'HV(A|B)'
                    THEN 'grapes'
                    ELSE NULL
                END]::TEXT[], -- commod
                ARRAY[]::TEXT[], -- manage
                ARRAY[lcdb_.source_data, linz_dvr_.source_data]::TEXT[], -- source_data
                range_merge(datemultirange(
                    VARIADIC ARRAY_REMOVE(ARRAY[
                        lcdb_.source_date,
                        linz_dvr_.source_date
                    ], NULL)
                ))::daterange, -- source_date
                range_merge(int4multirange(
                    VARIADIC ARRAY_REMOVE(ARRAY[
                        lcdb_.source_scale,
                        linz_dvr_.source_scale
                    ], NULL)
                ))::int4range,
                NULL
            )::nzlum_type
            WHEN linz_dvr_.improvements_description ~ '\mORCHARD\M'
            THEN ROW(
                ARRAY[]::TEXT[], -- lu_code_ancillary
                CASE
                    WHEN linz_dvr_.actual_property_use = '15'
                    THEN 1
                    WHEN linz_dvr_.improvements_description ~ '\mORCHARD\M'
                    THEN 2
                    WHEN linz_dvr_.category ~ '^H(C|K|P|S|V)(A|B|C)'
                    THEN 2
                    WHEN linz_dvr_.category ~ '^H(C|K|P|S|V)(D|E|F)?'
                    THEN 5
                    WHEN linz_dvr_.category ~ '^HX'
                    THEN 5
                    WHEN linz_dvr_.category ~ '^H(B|F|M)'
                    THEN 9
                    ELSE 6
                END,  -- confidence
                ARRAY[CASE
                    WHEN linz_dvr_.category ~ 'HK(A|B)'
                    THEN 'kiwifruit'
                    WHEN linz_dvr_.category ~ 'HV(A|B)'
                    THEN 'grapes'
                    ELSE NULL
                END]::TEXT[], -- commod
                ARRAY[]::TEXT[], -- manage
                ARRAY[linz_dvr_.source_data]::TEXT[], -- source_data
                linz_dvr_.source_date,
                linz_dvr_.source_scale,
                NULL
            )::nzlum_type
            WHEN hail_a10_.h3_index IS NOT NULL
            THEN ROW(
                ARRAY[]::TEXT[], -- lu_code_ancillary
                9, -- weak standalone; outer SELECT applies -1 → 8 final
                ARRAY[]::TEXT[], -- commod
                ARRAY[]::TEXT[], -- manage
                ARRAY[hail_a10_.source_data]::TEXT[],
                hail_a10_.source_date,
                hail_a10_.source_scale,
                NULL
            )::nzlum_type
            ELSE NULL
        END AS nzlum_type
        FROM roi
        LEFT JOIN (
            SELECT h3_index, source_data, source_date, source_scale
            FROM lum_
            WHERE lucid_2020 = '77 - Cropland - Orchards and vineyards (perennial)'
        ) AS lum_ ON roi.h3_index && lum_.h3_index
        LEFT JOIN (
            SELECT h3_index,
            daterange(
                '2011-05-02'::date,
                '2025-01-02'::date,
                '[]'
            ) AS source_date,
            '[60,100)'::int4range AS source_scale,
            'LINZ' AS source_data
            FROM topo50_orchards_h3
            WHERE :parent::h3index = h3_partition
        ) topo50_orchards_ ON roi.h3_index && topo50_orchards_.h3_index
        LEFT JOIN (
            SELECT h3_index, source_data, source_date, source_scale
            FROM lcdb_
            WHERE lcdb_.Class_2023 = 33 -- Orchard, Vineyard or Other Perennial Crop
        ) lcdb_ ON roi.h3_index && lcdb_.h3_index
        LEFT JOIN (
            SELECT
                h3_index,
                source_data,
                source_scale,
                source_date,
                actual_property_use,
                category,
                improvements_description
            FROM linz_dvr_
            WHERE actual_property_use IN (
                '01', -- Mixed, rural
                '15' -- Market gardens and orchards
            )
            OR category ~ '^H'
            OR improvements_description ~ '\mORCHARD\M'
            -- H(B|F|M) -- LOW
            -- H(X) -- MED
            -- H(C|K|P|S|V) -- HIGH
            -- Consider H*(A|B|C|D|E|F) too (quality)
        ) linz_dvr_ ON roi.h3_index && linz_dvr_.h3_index
        LEFT JOIN crop_perennial ON roi.h3_index && crop_perennial.h3_index
        LEFT JOIN (
            SELECT h3_index, source_data, source_date, source_scale
            FROM hail
            WHERE hail_category_ids && ARRAY['A10']
        ) hail_a10_ ON roi.h3_index && hail_a10_.h3_index
        WHERE lum_.h3_index IS NOT NULL
           OR topo50_orchards_.h3_index IS NOT NULL
           OR lcdb_.h3_index IS NOT NULL
           OR linz_dvr_.h3_index IS NOT NULL
           OR crop_perennial.h3_index IS NOT NULL
           OR hail_a10_.h3_index IS NOT NULL
    )
    SELECT
        base_classification.h3_index,
        lu_code_primary,
        lu_code_secondary,
        lu_code_tertiary,
        ROW(
            (nzlum_type).lu_code_ancillary,
            -- Clamp the confidence value, preserving nulls
            CASE
                WHEN (nzlum_type).confidence IS NULL
                THEN NULL
                ELSE LEAST(GREATEST((nzlum_type).confidence
                    + CASE
                        WHEN irrigation_.h3_index IS NOT NULL
                        THEN -2
                        ELSE 0
                    END
                    + CASE
                        WHEN crop_perennial.h3_index IS NOT NULL
                        THEN -6
                        ELSE 0
                    END
                    + CASE
                        WHEN hail_a10_.h3_index IS NOT NULL
                        THEN -1
                        ELSE 0
                    END
                    + CASE -- Penalise confidence by irrigation age (older mapping = less certain)
                        WHEN irrigation_.irrigation_age IS NULL THEN 0
                        WHEN irrigation_.irrigation_age <= 5    THEN 0
                        WHEN irrigation_.irrigation_age <= 10   THEN 1
                        WHEN irrigation_.irrigation_age <= 20   THEN 2
                        ELSE 3
                    END, 1), 12) -- confidence
            END,
            COALESCE((nzlum_type).commod, ARRAY[]::TEXT[]) || COALESCE(crop_perennial.commod, ARRAY[]::TEXT[]),
            COALESCE((nzlum_type).manage, ARRAY[]::TEXT[]) || COALESCE(ARRAY[irrigation_.manage], ARRAY[]::TEXT[]),
            COALESCE((nzlum_type).source_data, ARRAY[]::TEXT[]) || COALESCE(ARRAY[irrigation_.source_data]::TEXT[], ARRAY[]::TEXT[]) || COALESCE(ARRAY[crop_perennial.source_data]::TEXT[], ARRAY[]::TEXT[]),
            range_merge(datemultirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    (nzlum_type).source_date,
                    irrigation_.source_date,
                    crop_perennial.source_date
                ], NULL)
            ))::daterange, -- source_date
            range_merge(int4multirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    (nzlum_type).source_scale,
                    irrigation_.source_scale,
                    crop_perennial.source_scale
                ], NULL)
            ))::int4range, -- source_scale
            (nzlum_type).comment
        )::nzlum_type AS nzlum_type
    FROM base_classification
    LEFT JOIN crop_perennial ON base_classification.h3_index && crop_perennial.h3_index
    LEFT JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale,
            manage,
            irrigation_age
        FROM irrigation_
        WHERE irrigation_type ~ '^Drip'
    ) irrigation_ ON base_classification.h3_index && irrigation_.h3_index
    LEFT JOIN (
        SELECT h3_index, source_data, source_date, source_scale
        FROM hail
        WHERE hail_category_ids && ARRAY['A10']
    ) hail_a10_ ON base_classification.h3_index && hail_a10_.h3_index
);

-- Topo50. Middling confidence on its own. "Vegetation defined as pip or stone fruit eg apples, apricots, olives etc". No ancillary information available.

-- LUM: 77 Perennial cropland ('includes all orchards and vineyards')
-- LCDB 33
-- DVR: 15, H% (HB?, HC, HF?, HG?, HK, HP, HS, HV, HX?
-- HV crop type 'grapes'; HK 'kiwifruit'
-- irrigation, esp. drip/micro; check notes

-- TODO winter cover (see NRC), e.g. 522 (kiwifruit), 131 (Crop covers/bird nets/glass or plastic houses... there are perhaps class 2.5.0)

-- TODO Regional data
-- NRC olives_2023, macadamias_2023, nrc_kumara_growers_2023

-- TODO Special case data
-- NRC qa_avocado_property; title NA981/87 (QA)
-- title_no <> '524866' -- https://github.com/manaakiwhenua/luis-config/issues/5 (QA: gross error in the ratings database; a large forest block assigned 15 and HKD)

-- TODO need to heavily penalise anything that has pasture cover indicated... can see example of a farm near Otorohanga that has ORCHARD and is probably H%, but this is only part of the farm and the rest is pasture. LINZ topo etc includes the orchard partl this part is confidence 1, but the rest only confidence 2 despite being pasture cover.