CREATE TEMPORARY VIEW hail AS (
    WITH _env_southland_hail AS (
        SELECT
            h3_index,
            hail_category_count,
            hail_category_ids,
            -- geom_area_ha,
            source_date,
            source_data,
            source_scale
        FROM es_slu
        INNER JOIN es_slu_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ),
    _hbrc_hail AS (
        SELECT h3_index, hail_category_count, hail_category_ids, source_date, source_data, source_scale
        FROM hbrc_hail_suitable_remediated
        INNER JOIN hbrc_hail_suitable_remediated_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition

        UNION ALL

        SELECT h3_index, hail_category_count, hail_category_ids, source_date, source_data, source_scale
        FROM hbrc_hail_suitable_natural_state
        INNER JOIN hbrc_hail_suitable_natural_state_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition

        UNION ALL

        SELECT h3_index, hail_category_count, hail_category_ids, source_date, source_data, source_scale
        FROM hbrc_hail_risk_not_quantified
        INNER JOIN hbrc_hail_risk_not_quantified_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition

        UNION ALL

        SELECT h3_index, hail_category_count, hail_category_ids, source_date, source_data, source_scale
        FROM hbrc_hail_managed_for_land_use
        INNER JOIN hbrc_hail_managed_for_land_use_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition

        UNION ALL

        SELECT h3_index, hail_category_count, hail_category_ids, source_date, source_data, source_scale
        FROM hbrc_hail_contaminated_for_lu_human
        INNER JOIN hbrc_hail_contaminated_for_lu_human_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition

        UNION ALL

        SELECT h3_index, hail_category_count, hail_category_ids, source_date, source_data, source_scale
        FROM hbrc_hail_contaminated_for_lu_env
        INNER JOIN hbrc_hail_contaminated_for_lu_env_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition

        UNION ALL

        SELECT h3_index, hail_category_count, hail_category_ids, source_date, source_data, source_scale
        FROM hbrc_hail_background_natural_state
        INNER JOIN hbrc_hail_background_natural_state_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    )
    SELECT * FROM _env_southland_hail
    UNION ALL
    SELECT * FROM _hbrc_hail
);