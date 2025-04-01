CREATE TEMPORARY VIEW class_240 AS ( -- Perennial horticulture
    WITH base_classification AS (
        SELECT h3_index,
        2 AS lu_code_primary,
        4 AS lu_code_secondary,
        0 AS lu_code_tertiary,
        CASE
            WHEN (
                lum_.h3_index IS NOT NULL
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
                ARRAY[lum_.source_data, topo50_orchards_.source_data, linz_dvr_.source_data]::TEXT[], -- source_data
                range_merge(datemultirange(
                    VARIADIC ARRAY_REMOVE(ARRAY[
                        topo50_orchards_.source_date,
                        lum_.source_date,
                        linz_dvr_.source_date
                    ], NULL)
                ))::daterange, -- source_date
                range_merge(int4multirange(
                    VARIADIC ARRAY_REMOVE(ARRAY[
                        topo50_orchards_.source_scale,
                        lum_.source_scale,
                        linz_dvr_.source_scale
                    ], NULL)
                ))::int4range -- source_scale
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
                ARRAY[lcdb_.source_data, topo50_orchards_.source_data]::TEXT[], -- source_data
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
                ))::int4range -- source_scale
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
                ))::int4range -- source_scale
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
                ))::int4range
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
                ))::int4range
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
                linz_dvr_.source_scale
            )::nzlum_type
            ELSE NULL
        END AS nzlum_type
        FROM (
            SELECT *
            FROM lum_
            WHERE lucid_2020 = '77 - Cropland - Orchards and vineyards (perennial)'
        ) AS lum_
        FULL OUTER JOIN (
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
        ) topo50_orchards_ USING (h3_index)
        FULL OUTER JOIN (
            SELECT *
            FROM lcdb_
            WHERE lcdb_.Class_2018 = 33 -- Orchard, Vineyard or Other Perennial Crop
        ) lcdb_ USING (h3_index)
        FULL OUTER JOIN (
            SELECT *
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
        ) linz_dvr_ USING (h3_index)
    )
    SELECT
        h3_index,
        lu_code_primary,
        lu_code_secondary,
        lu_code_tertiary,
        ROW(
            (nzlum_type).lu_code_ancillary,
            -- Clamp the confidence value
            LEAST(GREATEST(CASE
                WHEN irrigation_.h3_index IS NOT NULL
                THEN ((nzlum_type).confidence - 2)
                ELSE (nzlum_type).confidence
            END, 1), 12),
            (nzlum_type).commod,
            COALESCE((nzlum_type).manage, '{}') || irrigation_.manage,
            (nzlum_type).source_data || irrigation_.source_data,
            CASE
                WHEN irrigation_.h3_index IS NOT NULL
                THEN range_merge(
                    irrigation_.source_date,
                    (nzlum_type).source_date)
                ELSE (nzlum_type).source_date
            END,
            CASE
                WHEN irrigation_.h3_index IS NOT NULL
                THEN range_merge(
                    irrigation_.source_scale,
                    (nzlum_type).source_scale
                )
                ELSE (nzlum_type).source_scale
            END
        )::nzlum_type AS nzlum_type
    FROM base_classification
    JOIN (
        SELECT *
        FROM irrigation_
        WHERE "type" IN (
            'Drip/micro',
            'Drip/Micro',
            'Drip/ Micro'
        )
    ) irrigation_ USING (h3_index)
);

-- Topo50. Middling confidence on its own. "Vegetation defined as pip or stone fruit eg apples, apricots, olives etc". No ancillary information available.

-- LUM: 77 Perennial cropland ('includes all orchards and vineyards')
-- LCDB 33
-- DVR: 15, H% (HB?, HC, HF?, HG?, HK, HP, HS, HV, HX?
-- HV crop type 'grapes'; HK 'kiwifruit'
-- irrigation, esp. drip/micro; check notes

-- TODO winter cover (see NRC), e.g. 522 (kiwifruit), 131 (Crop covers/bird nets/glass or plastic houses... there are perhaps class 2.5.0)

-- TODO Regional data
-- NRC qa_avocado_property; title NA981/87 (QA)
-- NRC olives_2023, macadamias_2023, nrc_kumara_growers_2023

-- TODO Special case data
-- title_no <> '524866' -- https://github.com/manaakiwhenua/luis-config/issues/5 (QA: gross error in the ratings database; a large forest block assigned 15 and HKD)