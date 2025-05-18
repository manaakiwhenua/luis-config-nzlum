CREATE TEMPORARY VIEW crop_maps AS
WITH gdc_crops AS (
    SELECT
        h3_index,
        h3_partition,
        crop,
        commod,
        manage,
        source_date,
        source_data,
        source_scale
    FROM summer_crop_gdc
    JOIN summer_crop_gdc_h3 USING (ogc_fid)
    WHERE
        :parent::h3index = h3_partition
        AND YearCompleted::INT = 2025 -- Most recent survey year

    UNION ALL

    SELECT
        h3_index,
        h3_partition,
        crop,
        commod,
        manage,
        source_date,
        source_data,
        source_scale
    FROM winter_crop_gdc
    JOIN winter_crop_gdc_h3 USING (ogc_fid)
    WHERE
        :parent::h3index = h3_partition
        AND EXTRACT(YEAR FROM "start_date") = 2021 -- Most recent survey year
),
gdf_crops_unnested AS (
    SELECT
    h3_index,
    h3_partition,
    crop,
    commod_item,
    manage_item,
    source_data,
    source_date,
    source_scale
    FROM gdc_crops
    LEFT JOIN LATERAL unnest(commod) AS commod_item ON true
    LEFT JOIN LATERAL unnest(manage) AS manage_item ON true
)
SELECT
    h3_index,
    h3_partition,
    deduplicate_and_sort(array_agg(DISTINCT crop)) AS crop,
    deduplicate_and_sort(array_agg(DISTINCT commod_item)) AS commod,
    deduplicate_and_sort(array_agg(DISTINCT manage_item)) AS manage,
    daterange_span(array_agg(source_date)) AS source_date,
    min(source_data) AS source_data,
    int4range_span(array_agg(source_scale)) AS source_scale
FROM gdf_crops_unnested
GROUP BY h3_index, h3_partition;