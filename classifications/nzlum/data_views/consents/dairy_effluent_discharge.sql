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
            fmDate DESC NULLS LAST, -- Most recently issued
            Expires DESC NULLS LAST, -- Latest expiry
            Area_HA DESC NULLS LAST, -- Largest
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
            'irrigation' AS manage
        FROM hbrc_all_consent_polygons -- Spatially imprecise
        INNER JOIN hbrc_all_consent_polygons_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND ActPrimaryIndustry = 'Agriculture - Dairying'
        AND ActPrimaryPurpose = 'Wastewater - Untreated'
        AND AuthorisationActivityType = 'Discharge Permit'
        ORDER BY 
            h3_index,
            DecisionServedDate DESC NULLS LAST, -- Most recently issued
            ExpiryDate DESC NULLS LAST, -- Latest expiry
            Area_m2 DESC NULLS LAST, -- Largest
            DocumentLink DESC NULLS LAST -- Prefer with more information
    )
    SELECT * FROM ecan_effluent_discharge
    UNION ALL
    SELECT * FROM hbrc_effluent_discharge
);