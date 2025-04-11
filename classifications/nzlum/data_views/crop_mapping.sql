CREATE TEMPORARY VIEW crop_maps AS (
    SELECT
        DISTINCT ON (h3_index)
        h3_index,
        crop, -- Source input, TEXT
        commod, -- Controlled vocab, TEXT[]
        manage, -- Controlled vocab, TEXT[]
        source_date,
        source_data,
        source_scale
    FROM sumcrop_2024_25_tairawhiti
    JOIN sumcrop_2024_25_tairawhiti_h3 USING (ogc_fid)
    WHERE :parent::h3index = h3_partition
    ORDER BY 
        h3_index,
        area_ha DESC NULLS LAST -- Larger
);