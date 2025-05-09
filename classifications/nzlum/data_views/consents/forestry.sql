-- NB ensure to handle overlapping features
CREATE TEMPORARY VIEW consents_forestry AS
WITH forestry_hbrc AS (
    SELECT
        DISTINCT ON (h3_index)
        h3_index,
        source_date,
        source_data,
        source_scale,
        CASE ActPrimaryPurpose
            WHEN 'Forestry - Afforestation'
            THEN TRUE
            ELSE FALSE
        END AS afforestation_flag
        -- ActPrimaryIndustry,
        -- ActPrimaryPurpose
    FROM hbrc_all_consent_polygons -- Spatially imprecise
    INNER JOIN hbrc_all_consent_polygons_h3 USING (ogc_fid)
    WHERE :parent::h3index = h3_partition
    AND ActPrimaryIndustry = 'Forestry'
    ORDER BY 
        h3_index,
        DecisionServedDate DESC NULLS LAST, -- Most recently issued
        ExpiryDate DESC NULLS LAST, -- Latest expiry
        Area_m2 DESC NULLS LAST, -- Largest
        DocumentLink DESC NULLS LAST -- Prefer with more information
),
forestry_horizons AS (
    SELECT
        DISTINCT ON (h3_index)
        h3_index,
        source_date,
        source_data,
        source_scale,
        CASE
            WHEN Ath_PurPrim ~ '\mAfforestation\M'
            THEN TRUE
            ELSE FALSE
        END AS afforestation_flag
        -- Ath_IndPrim,
        -- Ath_PurPrim
    FROM horizons_reg_act 
    INNER JOIN horizons_reg_act_h3 USING (ogc_fid)
    WHERE :parent::h3index = h3_partition
    AND Ath_IndPrim = 'Forestry'
    AND (
        Ath_PurPrim IS NULL
        OR (
            Ath_PurPrim NOT LIKE 'Construction | Access Road %'
            AND Ath_PurPrim NOT LIKE 'Manufacturing %'
            AND Ath_PurPrim NOT LIKE 'Mining %'
        )
    )
    ORDER BY 
        h3_index,
        Ath_Commence DESC NULLS LAST, -- Most recently issued
        Ath_Expiry DESC NULLS LAST, -- Latest expiry
        Shape__Area DESC NULLS LAST, -- Largest
        CASE ath_status
            WHEN 'Active'
            THEN 1
            WHEN 'Current'
            THEN 1
            WHEN 'Decision Served'
            THEN 2
            WHEN 'Lodged'
            THEN 2
            WHEN 'Not Yet Commenced'
            THEN 2
            WHEN 'On Hold'
            THEN 3
            WHEN 'Proposed'
            THEN 4
            WHEN 'Expired'
            THEN 5
            WHEN 'Surrendered'
            THEN 5
            ELSE NULL
        END ASC NULLS LAST
)
SELECT * FROM forestry_hbrc
UNION ALL
SELECT * FROM forestry_horizons;

-- TODO ActPrimaryPurpose = 'Forestry - Afforestation' OR 'Mechanical Land Preparation' (transitionary class?)
-- TODO harvesting consent = commodity = {sawlogs,pulpwood}?