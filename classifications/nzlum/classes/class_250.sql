-- NB validation area over E17605N58855
-- Production nurseries
-- Glasshouses/shadehouses
CREATE TEMPORARY VIEW class_250 AS ( -- Intensive horticulture
    SELECT h3_index,
    2 AS lu_code_primary,
    5 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    ROW(
        ARRAY[]::TEXT[], -- lu_code_ancillary
        LEAST(GREATEST(CASE
            WHEN crop_nurseries.h3_index IS NOT NULL
            THEN 1
            WHEN category ~ '^HG(A)'
            THEN 3
            WHEN category ~ '^HG(B)'
            THEN 4
            WHEN category ~ '^HG(C)'
            THEN 6
            WHEN category ~ '^HG(D)'
            THEN 8
            WHEN category ~ '^HG(E|F)?'
            THEN 11
            WHEN category ~ '^H(F|M|X)(A|B)'
            THEN 7
            WHEN category ~ '^H(F|M|X)(C|D|E|F)?'
            THEN 9
            ELSE 14 -- Start with extreme unconfidence otherwise
        END
        +
        CASE
            WHEN actual_property_use IN ('01', '15')
            THEN -2 -- Actual use supports the classification
            ELSE 2
        END
        +
        CASE -- TODO improvements greenhouse, shadehouse, nursery
            WHEN
            improvements_description ~ '\m(GREEN|GRN|SHADE|SHD|GLASS)\s?(HOUSE|HSE)\M'
            THEN -3 -- More confidence for greenhouse or shadehouse
            WHEN improvements_description ~ '\mNURSERY\M'
            THEN -3 -- More confidence for a (plant) nursery; NB that this term is also occasionally used for childcare, so consider zoning too
            ELSE 1 -- Less confidence otherwise
        END
        +
        CASE
            WHEN "zone" ~ '1*' -- Rural
            THEN -2 -- More confidence for rural zone
            WHEN "zone" ~ '2*' -- Lifestyle
            THEN 0 -- Neutral for lifestyle
            WHEN (
                "zone" ~ '3*' -- Other specific zone
                OR "zone" ~ '6*' -- Other broad zone
                OR "zone" = '0X' -- More than one zone
            )
            THEN 0 -- Neutral for unclear zones
            ELSE 2 -- Less confidence for all other zones
        END
        +
        CASE
            WHEN irrigation_.h3_index IS NOT NULL
            THEN -1
            ELSE 0
        END, 1), 12), -- Confidence
        ARRAY[]::TEXT[], -- commod
        ARRAY[irrigation_.manage]::TEXT[] || COALESCE(crop_nurseries.manage, ARRAY[]::TEXT[]), -- manage
        ARRAY[
            linz_dvr_.source_data,
            irrigation_.source_data,
            crop_nurseries.source_data
        ]::TEXT[], -- source_data
        range_merge(datemultirange(
            VARIADIC ARRAY_REMOVE(ARRAY[
                irrigation_.source_date,
                linz_dvr_.source_date,
                crop_nurseries.source_date
            ], NULL)
        ))::daterange, -- source_date
        range_merge(int4multirange(
            VARIADIC ARRAY_REMOVE(ARRAY[
                irrigation_.source_scale,
                linz_dvr_.source_scale,
                crop_nurseries.source_scale
            ], NULL)
        ))::int4range, -- source_scale
        NULL
    )::nzlum_type AS nzlum_type
    FROM (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale,
            category,
            actual_property_use,
            improvements_description,
            "zone"
        FROM linz_dvr_
        WHERE
            category ~ '^H'
            OR actual_property_use IN ('0', '00', '01', '1', '10', '15')
            OR improvements_description ~ '\m((GREEN|GRN|SHADE|SHD|GLASS)\s?(HOUSE|HSE))|NURSERY\M'
    ) linz_dvr_
    FULL OUTER JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale,
            manage
        FROM crop_maps
        WHERE source_data = 'GDC'
        AND crop IN (
            'Pine Nursery',
            'Grape Nursery',
            'Poplar/Willow Nursery'
        )
    ) AS crop_nurseries USING (h3_index)
    LEFT JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale,
            manage
        FROM irrigation_
        WHERE :parent::h3index = h3_partition
        AND irrigation_type ~ '^Drip'
    ) irrigation_ USING (h3_index)
);


--Use DVR
--      But this requires care, as actual use 15 is "market gardens and orchards" -- orchards are perennial and so are class 2.4.0; market gardens are part of 2.3.0
--      Use category HG (glasshouses) esp sub-types A/B, less so C, and NOT D/E/F (uneconomic and/or unimproved)

-- TODO find and add other sources of data, e.g. Stella/Alex A.'s data

-- TODO irrigation?, esp. drip/micro
