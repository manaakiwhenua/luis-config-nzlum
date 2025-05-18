CREATE TEMPORARY VIEW linz_dvr_ AS (
    SELECT DISTINCT ON (h3_index) h3_index,
    h3_partition,
    improvements_value,
    improvements_value_ratio,
    legal_description,
    gt_half_acre,
    land_area,
    property_category as category,
    actual_property_use,
    daterange(
        current_effective_valuation_date::DATE,
        current_effective_valuation_date::DATE,
        '[]'::TEXT
    ) AS source_date,
    improvements_description,
    ts_improvements_description,
    zoning AS "zone",
    nzptl.type AS land_estate,
    ownership_code,
    CASE
        WHEN urban_rural_2025_.IUR2025_V1_00 IN (
            '11', -- major urban
            '12', -- large urban
            '13', -- medium urban
            '14' -- small urban
        ) -- 0.1 - 1 m in urban areas
        THEN '(0,1]'::int4range
        WHEN urban_rural_2025_.IUR2025_V1_00 IN (
            '21', -- rural settlement
            '22' -- rural other
        ) -- 1 - 100 m in rural areas
        THEN '[1,100]'::int4range
        -- inland water, inlet, oceanic
        ELSE '[1,100]'::int4range
    END AS source_scale,
    'DVR' AS source_data
    FROM unit_of_property_h3 AS uop_h3
    LEFT JOIN unit_of_property AS uop USING (ogc_fid)
    LEFT JOIN national_dvr USING (unit_of_property_id)
    LEFT JOIN property_title_reference AS ptr USING (unit_of_property_id)
    LEFT JOIN nz_property_titles_list AS nzptl USING (title_no)
    LEFT JOIN (
        SELECT
            urban_rural_2025_h3.h3_index,
            urban_rural_2025.IUR2025_V1_00
        FROM urban_rural_2025_h3
        JOIN urban_rural_2025 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ) AS urban_rural_2025_ USING (h3_index)
    WHERE :parent::h3index = h3_partition
    AND urban_rural_2025_.IUR2025_V1_00 NOT IN (
        '31', -- Inland water (i.e. lakes)
        '32', -- Inlet
        '33' -- Oceanic
    )
    ORDER BY
        h3_index,
        current_effective_valuation_date DESC NULLS LAST, -- Take more recent valuation
        CASE
            WHEN actual_property_use ~ '^0' -- Mixed use
            THEN 3
            WHEN actual_property_use ~ '^(1-9)0' -- Mixed use
            THEN 2
            ELSE 1
        END ASC NULLS LAST, -- Prefer information about non-mixed uses 
        CASE
            WHEN actual_property_use = '45' -- Community services - Defence
            THEN 0
            ELSE 1
        END ASC NULLS LAST, -- Prefer information about defence as a special case
        capital_value / nullif(land_area, 0) DESC NULLS LAST, -- Take property with greater capital value per unit area,
        improvements_value DESC NULLS LAST, -- Take property with greater improvements value
        "zone" ASC NULLS LAST,
        land_estate ASC NULLS LAST
);

--TODO this may resolve faster if materialised, and partitioned by h3_parent
-- Actually, all of the 'data views' probably would.

-- NB some list of improvement codes:
-- www.aucklandcouncil.govt.nz/property-rates-valuations/our-valuation-of-your-property/Pages/description-of-improvement-codes.aspx
-- eservices.kapiticoast.govt.nz/rates/improvements
-- www.tauranga.govt.nz/living/property-and-rates/property-search/nature-of-improvements
-- www.selwyn.govt.nz/services/rates/property-search/nature-of-improvements