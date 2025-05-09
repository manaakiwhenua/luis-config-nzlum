-- NB ensure to handle overlapping features
CREATE TEMPORARY VIEW pastoral_consents AS
WITH hbrc_pastoral AS (
    SELECT
        DISTINCT ON (h3_index)
        h3_index,
        source_date,
        source_data,
        source_scale,
        CASE
            WHEN
            ActPrimaryPurpose = 'Feedpad / Feedlot'
            THEN TRUE
            ELSE FALSE
        END AS intensive,
        (CASE
            WHEN ActPrimaryIndustry = 'Agriculture - Deer' OR ActSecondaryIndustry = 'Agriculture - Deer'
            THEN ARRAY['deer']::TEXT[]
            ELSE ARRAY[]::TEXT[]
        END || CASE
            WHEN ActPrimaryIndustry = 'Agriculture - Dairying' OR ActSecondaryIndustry = 'Agriculture - Dairying'
            THEN ARRAY['cattle dairy']::TEXT[]
            ELSE ARRAY[]::TEXT[]
        END || CASE
            -- NB "Sheep /Beef" is not a typo
            WHEN ActPrimaryIndustry = 'Agriculture - Sheep /Beef' OR ActSecondaryIndustry = 'Agriculture - Sheep /Beef'
            THEN ARRAY['sheep', 'cattle meat']::TEXT[]
            ELSE ARRAY[]::TEXT[]
        END || CASE
            WHEN ActPrimaryIndustry = 'Agriculture - Beef' OR ActSecondaryIndustry = 'Agriculture - Beef'
            THEN ARRAY['cattle meat']::TEXT[]
            ELSE ARRAY[]::TEXT[]
        END) AS commod,
        CASE
            WHEN ActPrimaryPurpose = 'Water Supply - Irrigation'
            THEN ARRAY['irrigation']
            ELSE NULL
        END AS manage
    FROM hbrc_all_consent_polygons -- Spatially imprecise
    INNER JOIN hbrc_all_consent_polygons_h3 USING (ogc_fid)
    WHERE :parent::h3index = h3_partition
    AND ActPrimaryIndustry ~ '^Agriculture - (Sheep /Beef|Dairying|Beef|Pastoral Farming)'
    AND ActPrimaryPurpose !~ '^Forestry'
    -- TODO ActPrimaryPurpose = 'Forestry - Afforestation' OR 'Mechanical Land Preparation' (transitionary class?)
    -- TODO harvesting consent = commodity = {sawlogs,pulpwood}?
    ORDER BY 
        h3_index,
        DecisionServedDate DESC NULLS LAST, -- Most recently issued
        ExpiryDate DESC NULLS LAST, -- Latest expiry
        Area_m2 DESC NULLS LAST, -- Largest
        DocumentLink DESC NULLS LAST -- Prefer with more information
), horizons_pastoral AS (
    SELECT DISTINCT ON (h3_index)
        h3_index,
        source_date,
        source_data,
        source_scale,
        CASE
            WHEN ATH_PURPRIM ~ '^Agriculture, Intensive Farming' OR ATH_PURSEC ~ '^Agriculture, Intensive Farming'
                THEN TRUE
            ELSE FALSE
        END AS intensive,
        (CASE
            WHEN ATH_PURPRIM ~ '^Agriculture.*\mBeef\M' OR ATH_PURSEC ~ '^Agriculture.*\mBeef\M'
            THEN ARRAY['cattle meat']
            ELSE ARRAY[]::TEXT[]
        END || CASE
            WHEN ATH_PURPRIM ~ '^Agriculture.*\mSheep\M' OR ATH_PURSEC ~ '^Agriculture.*\mSheep\M'
            THEN ARRAY['sheep']
            ELSE ARRAY[]::TEXT[]
        END || CASE
            WHEN ATH_PURPRIM ~ '^Agriculture.*\mDairy Goat\M' OR ATH_PURSEC ~ '^Agriculture.*\mDairy Goat\M'
            THEN ARRAY['goats dairy']
            WHEN ATH_PURPRIM ~ '^Agriculture.*\mGoat\M' OR ATH_PURSEC ~ '^Agriculture.*\mGoat\M'
            THEN ARRAY['goats']
            ELSE ARRAY[]::TEXT[]
        END || CASE
            WHEN ATH_PURPRIM ~ '^Agriculture.*\mDairy Cattle\M' OR ATH_PURSEC ~ '^Agriculture.*\mDairy Cattle\M'
            THEN ARRAY['cattle dairy']
            WHEN ATH_PURPRIM ~ '^Agriculture.*\mIntensive Farming, Dairy\M' OR ATH_PURSEC ~ '^Agriculture.*\mIntensive Farming, Dairy\M'
            THEN ARRAY['cattle dairy']
            ELSE ARRAY[]::TEXT[]
        END
        ) AS commod,
        CASE
            WHEN ATH_TYPE = 'Water Permit' AND ATH_INDPRIM = 'Agriculture'
            THEN ARRAY['irrigation']
            ELSE NULL
        END AS manage
    FROM horizons_reg_act
    JOIN horizons_reg_act_h3 USING (ogc_fid)
    WHERE :parent::h3index = h3_partition
    AND (
        ATH_PURPRIM IN (
            'Agriculture | Beef Cattle Farming',
            'Agriculture | Dairy Cattle Farming',
            'Agriculture | Dairy Goat Farming',
            'Agriculture | Grains, Forage & Other Crop Cultivation',
            'Agriculture | Other Livestock Farming',
            'Agriculture | Pasture Cultivation (Irrigation, Animal Effluent, Fertiliser or Biosolids)',
            'Agriculture | Sheep & Beef Cattle Farming'
        ) OR ATH_PURSEC IN (
            'Agriculture | Beef Cattle Farming',
            'Agriculture | Dairy Cattle Farming',
            'Agriculture | Dairy Goat Farming',
            'Agriculture | Grains, Forage & Other Crop Cultivation',
            'Agriculture | Other Livestock Farming',
            'Agriculture | Pasture Cultivation (Irrigation, Animal Effluent, Fertiliser or Biosolids)',
            'Agriculture | Sheep & Beef Cattle Farming'
        )
    )
    ORDER BY 
        h3_index,
        ATH_EXPIRY DESC NULLS LAST, -- Most recently issued
        ATH_GRANTED DESC NULLS LAST -- Most recentlty granted
)
SELECT * FROM hbrc_pastoral
UNION ALL
SELECT * FROM horizons_pastoral;
