CREATE TEMPORARY VIEW dairy_effluent_discharge AS (
    WITH ecan_effluent_discharge AS (
        SELECT
            DISTINCT ON (h3_index)
            h3_index,
            h3_partition,
            source_date,
            source_data,
            source_scale,
            animal_count,
            manage
        FROM ecan_effluent_dairy_discharge_area_h3
        INNER JOIN ecan_effluent_dairy_discharge_area USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        ORDER BY
            h3_index,
            start_date DESC NULLS LAST,
            end_date DESC NULLS LAST,
            Area_HA DESC NULLS LAST,
            animal_count DESC NULLS LAST -- Most animals
    ),
    hbrc_effluent_discharge AS (
        SELECT
            DISTINCT ON (h3_index)
            h3_index,
            h3_partition,
            source_date,
            source_data,
            source_scale,
            NULL::INTEGER AS animal_count,
            ARRAY['irrigation']::TEXT[] AS manage
        FROM hbrc_all_consent_polygons -- Spatially imprecise
        INNER JOIN hbrc_all_consent_polygons_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND ActPrimaryIndustry = 'Agriculture - Dairying'
        AND ActPrimaryPurpose = 'Wastewater - Untreated'
        AND Authorisation_Activity_Type = 'Discharge Permit'
        ORDER BY
            h3_index,
            Authorisation_Decision_Date DESC NULLS LAST, -- Most recently issued
            Authorisation_Expiry_Date DESC NULLS LAST, -- Latest expiry
            Area_m2 DESC NULLS LAST, -- Largest
            Document_Link DESC NULLS LAST -- Prefer with more information
    ),
    wrc_dairy AS (
        SELECT DISTINCT ON (uop_h3.h3_index)
            uop_h3.h3_index,
            uop_h3.h3_partition,
            c.source_date,
            c.source_data,
            c.source_scale,
            NULL::INTEGER AS animal_count,
            ARRAY[]::TEXT[] AS manage
        FROM wrc_land_use_consents c
        JOIN wrc_land_use_consents_h3 c_h3 ON c.ogc_fid = c_h3.ogc_fid
        JOIN unit_of_property_h3 uop_inner_h3 ON c_h3.h3_index && uop_inner_h3.h3_index
        JOIN unit_of_property uop_inner ON uop_inner_h3.ogc_fid = uop_inner.ogc_fid
            AND ST_Within(c.geom, uop_inner.geom)
        JOIN unit_of_property uop_all ON uop_inner.unit_of_property_id = uop_all.unit_of_property_id
        JOIN unit_of_property_h3 uop_h3 ON uop_all.ogc_fid = uop_h3.ogc_fid
        WHERE :parent::h3index = c_h3.h3_partition
        AND   :parent::h3index = uop_inner_h3.h3_partition
        AND   :parent::h3index = uop_h3.h3_partition
        AND c.STATUS IN ('Current', 'Expired - S.124 Protection')
        AND c.PRIMARY_INDUSTRY_PURPOSE = 'Agricultural farming - dairy'
        ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
    ),
    wrc_discharge_dairy AS (
        SELECT DISTINCT ON (uop_h3.h3_index)
            uop_h3.h3_index,
            uop_h3.h3_partition,
            c.source_date,
            c.source_data,
            c.source_scale,
            NULL::INTEGER AS animal_count,
            ARRAY[]::TEXT[] AS manage
        FROM wrc_discharge_permits c
        JOIN wrc_discharge_permits_h3 c_h3 ON c.ogc_fid = c_h3.ogc_fid
        JOIN unit_of_property_h3 uop_inner_h3 ON c_h3.h3_index && uop_inner_h3.h3_index
        JOIN unit_of_property uop_inner ON uop_inner_h3.ogc_fid = uop_inner.ogc_fid
            AND ST_Within(c.geom, uop_inner.geom)
        JOIN unit_of_property uop_all ON uop_inner.unit_of_property_id = uop_all.unit_of_property_id
        JOIN unit_of_property_h3 uop_h3 ON uop_all.ogc_fid = uop_h3.ogc_fid
        WHERE :parent::h3index = c_h3.h3_partition
        AND   :parent::h3index = uop_inner_h3.h3_partition
        AND   :parent::h3index = uop_h3.h3_partition
        AND c.STATUS IN ('Current', 'Expired - S.124 Protection')
        AND c.PRIMARY_INDUSTRY_PURPOSE = 'Agricultural farming - dairy'
        ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
    ),
    gwrc_dairy AS (
        SELECT DISTINCT ON (uop_h3.h3_index)
            uop_h3.h3_index,
            uop_h3.h3_partition,
            c.source_date,
            c.source_data,
            c.source_scale,
            NULL::INTEGER AS animal_count,
            ARRAY[]::TEXT[] AS manage
        FROM gwrc_resource_consents c
        JOIN gwrc_resource_consents_h3 c_h3 ON c.ogc_fid = c_h3.ogc_fid
        JOIN unit_of_property_h3 uop_inner_h3 ON c_h3.h3_index && uop_inner_h3.h3_index
        JOIN unit_of_property uop_inner ON uop_inner_h3.ogc_fid = uop_inner.ogc_fid
            AND ST_Within(c.geom, uop_inner.geom)
        JOIN unit_of_property uop_all ON uop_inner.unit_of_property_id = uop_all.unit_of_property_id
        JOIN unit_of_property_h3 uop_h3 ON uop_all.ogc_fid = uop_h3.ogc_fid
        WHERE :parent::h3index = c_h3.h3_partition
        AND   :parent::h3index = uop_inner_h3.h3_partition
        AND   :parent::h3index = uop_h3.h3_partition
        AND c.RCstatus = 'Granted'
        AND c.Purpose_Desc ~* '\bdairy shed effluent\b'
        AND c.RC_APT_DESC = 'DP - ANIMAL WASTE TO LAND'
        AND c.source_date IS NOT NULL
        ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
    ),
    orc_dairy AS (
        SELECT DISTINCT ON (uop_h3.h3_index)
            uop_h3.h3_index,
            uop_h3.h3_partition,
            c.source_date,
            c.source_data,
            c.source_scale,
            NULL::INTEGER AS animal_count,
            ARRAY[]::TEXT[] AS manage
        FROM orc_all_consents c
        JOIN orc_all_consents_h3 c_h3 ON c.ogc_fid = c_h3.ogc_fid
        JOIN unit_of_property_h3 uop_inner_h3 ON c_h3.h3_index && uop_inner_h3.h3_index
        JOIN unit_of_property uop_inner ON uop_inner_h3.ogc_fid = uop_inner.ogc_fid
            AND ST_Within(c.geom, uop_inner.geom)
        JOIN unit_of_property uop_all ON uop_inner.unit_of_property_id = uop_all.unit_of_property_id
        JOIN unit_of_property_h3 uop_h3 ON uop_all.ogc_fid = uop_h3.ogc_fid
        WHERE :parent::h3index = c_h3.h3_partition
        AND   :parent::h3index = uop_inner_h3.h3_partition
        AND   :parent::h3index = uop_h3.h3_partition
        AND c.Consent_Status IN ('Current', 'Current - Variation in App')
        AND c.activity_tsvector @@ phraseto_tsquery('english', 'dairy farm')
        ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
    ),
    mdc_dairy AS (
        SELECT DISTINCT ON (h3_index)
            h3_index,
            h3_partition,
            source_date,
            source_data,
            source_scale,
            NULL::INTEGER AS animal_count,
            ARRAY[]::TEXT[] AS manage
        FROM mdc_discharge_polygon
        JOIN mdc_discharge_polygon_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND Status = 'Current'
        AND ActivityType IN ('Dairy', 'Piggery & Dairy')
        ORDER BY h3_index
    ),
    mdc_dairy_point AS (
        SELECT DISTINCT ON (uop_h3.h3_index)
            uop_h3.h3_index,
            uop_h3.h3_partition,
            c.source_date,
            c.source_data,
            c.source_scale,
            NULL::INTEGER AS animal_count,
            ARRAY[]::TEXT[] AS manage
        FROM mdc_discharge_point c
        JOIN mdc_discharge_point_h3 c_h3 ON c.ogc_fid = c_h3.ogc_fid
        JOIN unit_of_property_h3 uop_inner_h3 ON c_h3.h3_index && uop_inner_h3.h3_index
        JOIN unit_of_property uop_inner ON uop_inner_h3.ogc_fid = uop_inner.ogc_fid
            AND ST_Within(c.geom, uop_inner.geom)
        JOIN unit_of_property uop_all ON uop_inner.unit_of_property_id = uop_all.unit_of_property_id
        JOIN unit_of_property_h3 uop_h3 ON uop_all.ogc_fid = uop_h3.ogc_fid
        WHERE :parent::h3index = c_h3.h3_partition
        AND   :parent::h3index = uop_inner_h3.h3_partition
        AND   :parent::h3index = uop_h3.h3_partition
        AND c.Status = 'Current'
        AND c.ActivityType IN ('Dairy', 'Piggery & Dairy')
        ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
    ),
    wcrc_shapes_dairy AS (
        SELECT DISTINCT ON (h3_index)
            h3_index,
            h3_partition,
            source_date,
            source_data,
            source_scale,
            NULL::INTEGER AS animal_count,
            ARRAY[]::TEXT[] AS manage
        FROM wcrc_consent_shapes
        JOIN wcrc_consent_shapes_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND CurrentStatus = 'Current'
        AND PrimaryIndustry = 'A0130 Dairy Cattle Farming'
        ORDER BY h3_index, upper(source_date) DESC NULLS LAST
    ),
    wcrc_points_dairy AS (
        SELECT DISTINCT ON (uop_h3.h3_index)
            uop_h3.h3_index,
            uop_h3.h3_partition,
            c.source_date,
            c.source_data,
            c.source_scale,
            NULL::INTEGER AS animal_count,
            ARRAY[]::TEXT[] AS manage
        FROM wcrc_consent_points c
        JOIN wcrc_consent_points_h3 c_h3 ON c.ogc_fid = c_h3.ogc_fid
        JOIN unit_of_property_h3 uop_inner_h3 ON c_h3.h3_index && uop_inner_h3.h3_index
        JOIN unit_of_property uop_inner ON uop_inner_h3.ogc_fid = uop_inner.ogc_fid
            AND ST_Within(c.geom, uop_inner.geom)
        JOIN unit_of_property uop_all ON uop_inner.unit_of_property_id = uop_all.unit_of_property_id
        JOIN unit_of_property_h3 uop_h3 ON uop_all.ogc_fid = uop_h3.ogc_fid
        WHERE :parent::h3index = c_h3.h3_partition
        AND   :parent::h3index = uop_inner_h3.h3_partition
        AND   :parent::h3index = uop_h3.h3_partition
        AND c.CurrentStatus = 'Current'
        AND c.PrimaryIndustry = 'A0130 Dairy Cattle Farming'
        ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
    ),
    bop_dairy AS (
        SELECT DISTINCT ON (uop_h3.h3_index)
            uop_h3.h3_index,
            uop_h3.h3_partition,
            c.source_date,
            c.source_data,
            c.source_scale,
            NULL::INTEGER AS animal_count,
            ARRAY[]::TEXT[] AS manage
        FROM bop_all_consents c
        JOIN bop_all_consents_h3 c_h3 ON c.ogc_fid = c_h3.ogc_fid
        JOIN unit_of_property_h3 uop_inner_h3 ON c_h3.h3_index && uop_inner_h3.h3_index
        JOIN unit_of_property uop_inner ON uop_inner_h3.ogc_fid = uop_inner.ogc_fid
            AND ST_Within(c.geom, uop_inner.geom)
        JOIN unit_of_property uop_all ON uop_inner.unit_of_property_id = uop_all.unit_of_property_id
        JOIN unit_of_property_h3 uop_h3 ON uop_all.ogc_fid = uop_h3.ogc_fid
        WHERE :parent::h3index = c_h3.h3_partition
        AND   :parent::h3index = uop_inner_h3.h3_partition
        AND   :parent::h3index = uop_h3.h3_partition
        AND c.Status = 'Current'
        AND c.Category = 'Dairy'
        ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
    ),
    ecan_effluent_discharge_point AS (
        SELECT DISTINCT ON (uop_h3.h3_index)
            uop_h3.h3_index,
            uop_h3.h3_partition,
            c.source_date,
            c.source_data,
            c.source_scale,
            c.animal_count,
            c.manage
        FROM ecan_effluent_dairy_discharge c
        JOIN ecan_effluent_dairy_discharge_h3 c_h3 ON c.ogc_fid = c_h3.ogc_fid
        JOIN unit_of_property_h3 uop_inner_h3 ON c_h3.h3_index && uop_inner_h3.h3_index
        JOIN unit_of_property uop_inner ON uop_inner_h3.ogc_fid = uop_inner.ogc_fid
            AND ST_Within(c.geom, uop_inner.geom)
        JOIN unit_of_property uop_all ON uop_inner.unit_of_property_id = uop_all.unit_of_property_id
        JOIN unit_of_property_h3 uop_h3 ON uop_all.ogc_fid = uop_h3.ogc_fid
        WHERE :parent::h3index = c_h3.h3_partition
        AND   :parent::h3index = uop_inner_h3.h3_partition
        AND   :parent::h3index = uop_h3.h3_partition
        ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST, c.animal_count DESC NULLS LAST
    )
    SELECT * FROM ecan_effluent_discharge
    UNION ALL
    SELECT * FROM ecan_effluent_discharge_point
    UNION ALL
    SELECT * FROM hbrc_effluent_discharge
    UNION ALL
    SELECT * FROM wrc_dairy
    UNION ALL
    SELECT * FROM wrc_discharge_dairy
    UNION ALL
    SELECT * FROM gwrc_dairy
    UNION ALL
    SELECT * FROM orc_dairy
    UNION ALL
    SELECT * FROM mdc_dairy
    UNION ALL
    SELECT * FROM mdc_dairy_point
    UNION ALL
    SELECT * FROM wcrc_shapes_dairy
    UNION ALL
    SELECT * FROM wcrc_points_dairy
    UNION ALL
    SELECT * FROM bop_dairy
);