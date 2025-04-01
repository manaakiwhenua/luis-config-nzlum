-- TODO handle overlapping features
CREATE TEMPORARY VIEW dairy_effluent_discharge AS (
    SELECT
        DISTINCT ON (h3_index) *,
        -- daterange(
        --     CURRENT_DATE,
        --     LEAST(CURRENT_DATE, Expires::DATE),
        --     '[]'
        -- ) AS source_date,
        daterange(
            fmDate::DATE,
            toDate::DATE,
            '[]'
        ) AS source_date,
        'ECAN' AS source_data,
        CASE FEATURE_ACCURACY -- Assume unknown/null corresponds to > 50 m
            WHEN '1 metre to 5 metres' THEN '[1,5]'::int4range
            WHEN '5 metres to 20 metres' THEN '[5,20]'::int4range
            WHEN 'Greater than 50 metres' THEN '(50,)'::int4range
            WHEN 'Unknown' THEN '(50,)'::int4range
            ELSE '(50,)'::int4range
        END AS source_scale
    FROM ecan_effluent_dairy_discharge_area_h3
    INNER JOIN ecan_effluent_dairy_discharge_area USING (ogc_fid)
    WHERE :parent::h3index = h3_partition
    ORDER BY 
        h3_index,
        fmDate DESC NULLS LAST, -- Most recently issued
        Expires DESC NULLS LAST, -- Latest expiry
        Area_HA DESC NULLS LAST, -- Largest
        ActualAnimalNumbers_int DESC NULLS LAST -- Most animals
);
