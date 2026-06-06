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
            WHEN ActPrimaryPurpose = 'Feedpad / Feedlot'
            THEN TRUE
            ELSE FALSE
        END AS intensive,
        (CASE
            WHEN ActPrimaryIndustry = 'Agriculture - Dairying'
              OR ActSecondaryIndustry = 'Agriculture - Dairying'
              OR AuthPrimaryIndustry = 'Agriculture - Dairying'
              OR AuthSecondaryIndustry = 'Agriculture - Dairying'
            THEN ARRAY['cattle dairy']::TEXT[]
            ELSE ARRAY[]::TEXT[]
        END || CASE
            WHEN ActPrimaryIndustry = 'Agriculture - Deer'
              OR ActSecondaryIndustry = 'Agriculture - Deer'
              OR AuthPrimaryIndustry = 'Agriculture - Deer'
              OR AuthSecondaryIndustry = 'Agriculture - Deer'
            THEN ARRAY['deer']::TEXT[]
            ELSE ARRAY[]::TEXT[]
        END || CASE
            -- NB space before "/" is real in some records; regex handles both variants
            WHEN ActPrimaryIndustry ~ 'Agriculture - Sheep ?.?/Beef'
              OR ActSecondaryIndustry ~ 'Agriculture - Sheep ?.?/Beef'
              OR AuthPrimaryIndustry ~ 'Agriculture - Sheep ?.?/Beef'
              OR AuthSecondaryIndustry ~ 'Agriculture - Sheep ?.?/Beef'
            THEN ARRAY['sheep', 'cattle meat']::TEXT[]
            ELSE ARRAY[]::TEXT[]
        END || CASE
            WHEN (ActPrimaryIndustry = 'Agriculture - Sheep'
               OR ActSecondaryIndustry = 'Agriculture - Sheep'
               OR AuthPrimaryIndustry = 'Agriculture - Sheep'
               OR AuthSecondaryIndustry = 'Agriculture - Sheep')
            AND NOT (ActPrimaryIndustry ~ 'Agriculture - Sheep ?.?/Beef'
                  OR AuthPrimaryIndustry ~ 'Agriculture - Sheep ?.?/Beef')
            THEN ARRAY['sheep']::TEXT[]
            ELSE ARRAY[]::TEXT[]
        END || CASE
            WHEN ActPrimaryIndustry = 'Agriculture - Beef'
              OR ActSecondaryIndustry = 'Agriculture - Beef'
              OR AuthPrimaryIndustry = 'Agriculture - Beef'
              OR AuthSecondaryIndustry = 'Agriculture - Beef'
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
    AND (
        ActPrimaryIndustry ~ '^Agriculture - (Sheep|Dairying|Beef|Pastoral Farming)'
        OR AuthPrimaryIndustry ~ '^Agriculture - (Sheep|Dairying|Beef|Pastoral Farming)'
    )
    AND ActPrimaryPurpose !~ '^Forestry'
    -- TODO ActPrimaryPurpose = 'Forestry - Afforestation' OR 'Mechanical Land Preparation' (transitionary class?)
    -- TODO harvesting consent = commodity = {sawlogs,pulpwood}?
    ORDER BY
        h3_index,
        Authorisation_Decision_Date DESC NULLS LAST, -- Most recently issued
        Authorisation_Expiry_Date DESC NULLS LAST, -- Latest expiry
        Area_m2 DESC NULLS LAST, -- Largest
        Document_Link DESC NULLS LAST -- Prefer with more information
), horizons_pastoral AS (
    SELECT DISTINCT ON (h3_index)
        h3_index,
        source_date,
        source_data,
        source_scale,
        CASE
            WHEN PRIMARY_PURPOSE ~ '^Agriculture, Intensive Farming' OR SECONDARY_PURPOSE ~ '^Agriculture, Intensive Farming'
                THEN TRUE
            ELSE FALSE
        END AS intensive,
        (CASE
            WHEN PRIMARY_PURPOSE ~ '^Agriculture.*\mBeef\M' OR SECONDARY_PURPOSE ~ '^Agriculture.*\mBeef\M'
            THEN ARRAY['cattle meat']
            ELSE ARRAY[]::TEXT[]
        END || CASE
            WHEN PRIMARY_PURPOSE ~ '^Agriculture.*\mSheep\M' OR SECONDARY_PURPOSE ~ '^Agriculture.*\mSheep\M'
            THEN ARRAY['sheep']
            ELSE ARRAY[]::TEXT[]
        END || CASE
            WHEN PRIMARY_PURPOSE ~ '^Agriculture.*\mDairy Goat\M' OR SECONDARY_PURPOSE ~ '^Agriculture.*\mDairy Goat\M'
            THEN ARRAY['goats dairy']
            WHEN PRIMARY_PURPOSE ~ '^Agriculture.*\mGoat\M' OR SECONDARY_PURPOSE ~ '^Agriculture.*\mGoat\M'
            THEN ARRAY['goats']
            ELSE ARRAY[]::TEXT[]
        END || CASE
            WHEN PRIMARY_PURPOSE ~ '^Agriculture.*\mDairy Cattle\M' OR SECONDARY_PURPOSE ~ '^Agriculture.*\mDairy Cattle\M'
            THEN ARRAY['cattle dairy']
            WHEN PRIMARY_PURPOSE ~ '^Agriculture.*\mIntensive Farming, Dairy\M' OR SECONDARY_PURPOSE ~ '^Agriculture.*\mIntensive Farming, Dairy\M'
            THEN ARRAY['cattle dairy']
            ELSE ARRAY[]::TEXT[]
        END
        ) AS commod,
        CASE
            WHEN ACTIVITY_TYPE = 'Water Permit' AND PRIMARY_INDUSTRY = 'Agriculture'
            THEN ARRAY['irrigation']::TEXT[]
            ELSE NULL::TEXT[]
        END AS manage
    FROM horizons_reg_act
    JOIN horizons_reg_act_h3 USING (ogc_fid)
    WHERE :parent::h3index = h3_partition
    AND STATUS = 'Current'
    AND (
        PRIMARY_PURPOSE IN (
            'Agriculture | Beef Cattle Farming',
            'Agriculture | Dairy Cattle Farming',
            'Agriculture | Dairy Goat Farming',
            'Agriculture | Grains, Forage & Other Crop Cultivation',
            'Agriculture | Other Livestock Farming',
            'Agriculture | Pasture Cultivation (Irrigation, Animal Effluent, Fertiliser or Biosolids)',
            'Agriculture | Sheep & Beef Cattle Farming'
        ) OR SECONDARY_PURPOSE IN (
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
        EXPIRY_DATE DESC NULLS LAST,
        GRANTED_DATE DESC NULLS LAST
), horizons_pastoral_point AS (
    SELECT DISTINCT ON (uop_h3.h3_index)
        uop_h3.h3_index,
        c.source_date,
        c.source_data,
        c.source_scale,
        CASE
            WHEN c.PRIMARY_PURPOSE ~ '^Agriculture, Intensive Farming' OR c.SECONDARY_PURPOSE ~ '^Agriculture, Intensive Farming'
                THEN TRUE
            ELSE FALSE
        END AS intensive,
        (CASE
            WHEN c.PRIMARY_PURPOSE ~ '^Agriculture.*\mBeef\M' OR c.SECONDARY_PURPOSE ~ '^Agriculture.*\mBeef\M'
            THEN ARRAY['cattle meat']
            ELSE ARRAY[]::TEXT[]
        END || CASE
            WHEN c.PRIMARY_PURPOSE ~ '^Agriculture.*\mSheep\M' OR c.SECONDARY_PURPOSE ~ '^Agriculture.*\mSheep\M'
            THEN ARRAY['sheep']
            ELSE ARRAY[]::TEXT[]
        END || CASE
            WHEN c.PRIMARY_PURPOSE ~ '^Agriculture.*\mDairy Goat\M' OR c.SECONDARY_PURPOSE ~ '^Agriculture.*\mDairy Goat\M'
            THEN ARRAY['goats dairy']
            WHEN c.PRIMARY_PURPOSE ~ '^Agriculture.*\mGoat\M' OR c.SECONDARY_PURPOSE ~ '^Agriculture.*\mGoat\M'
            THEN ARRAY['goats']
            ELSE ARRAY[]::TEXT[]
        END || CASE
            WHEN c.PRIMARY_PURPOSE ~ '^Agriculture.*\mDairy Cattle\M' OR c.SECONDARY_PURPOSE ~ '^Agriculture.*\mDairy Cattle\M'
            THEN ARRAY['cattle dairy']
            WHEN c.PRIMARY_PURPOSE ~ '^Agriculture.*\mIntensive Farming, Dairy\M' OR c.SECONDARY_PURPOSE ~ '^Agriculture.*\mIntensive Farming, Dairy\M'
            THEN ARRAY['cattle dairy']
            ELSE ARRAY[]::TEXT[]
        END) AS commod,
        CASE
            WHEN c.ACTIVITY_TYPE = 'Water Permit' AND c.PRIMARY_INDUSTRY = 'Agriculture'
            THEN ARRAY['irrigation']::TEXT[]
            ELSE NULL::TEXT[]
        END AS manage
    FROM horizons_reg_act_point c
    JOIN horizons_reg_act_point_h3 c_h3 ON c.ogc_fid = c_h3.ogc_fid
    JOIN unit_of_property_h3 uop_inner_h3 ON c_h3.h3_index && uop_inner_h3.h3_index
    JOIN unit_of_property uop_inner ON uop_inner_h3.ogc_fid = uop_inner.ogc_fid
        AND ST_Within(c.geom, uop_inner.geom)
    JOIN unit_of_property uop_all ON uop_inner.unit_of_property_id = uop_all.unit_of_property_id
    JOIN unit_of_property_h3 uop_h3 ON uop_all.ogc_fid = uop_h3.ogc_fid
    WHERE :parent::h3index = c_h3.h3_partition
    AND   :parent::h3index = uop_inner_h3.h3_partition
    AND   :parent::h3index = uop_h3.h3_partition
    AND c.STATUS = 'Current'
    AND (
        c.PRIMARY_PURPOSE IN (
            'Agriculture | Beef Cattle Farming',
            'Agriculture | Dairy Cattle Farming',
            'Agriculture | Dairy Goat Farming',
            'Agriculture | Grains, Forage & Other Crop Cultivation',
            'Agriculture | Other Livestock Farming',
            'Agriculture | Pasture Cultivation (Irrigation, Animal Effluent, Fertiliser or Biosolids)',
            'Agriculture | Sheep & Beef Cattle Farming'
        ) OR c.SECONDARY_PURPOSE IN (
            'Agriculture | Beef Cattle Farming',
            'Agriculture | Dairy Cattle Farming',
            'Agriculture | Dairy Goat Farming',
            'Agriculture | Grains, Forage & Other Crop Cultivation',
            'Agriculture | Other Livestock Farming',
            'Agriculture | Pasture Cultivation (Irrigation, Animal Effluent, Fertiliser or Biosolids)',
            'Agriculture | Sheep & Beef Cattle Farming'
        )
    )
    ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
), wrc_pastoral AS (
    SELECT DISTINCT ON (uop_h3.h3_index)
        uop_h3.h3_index,
        c.source_date,
        c.source_data,
        c.source_scale,
        FALSE AS intensive,
        CASE
            WHEN c.PRIMARY_INDUSTRY_PURPOSE = 'Agricultural farming - dairy'
            THEN ARRAY['cattle dairy']::TEXT[]
            ELSE ARRAY[]::TEXT[]
        END AS commod,
        NULL::TEXT[] AS manage
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
    AND c.PRIMARY_INDUSTRY_PURPOSE IN (
        'Agricultural farming - dairy',
        'Agricultural farming - general',
        'Agricultural farming - drystock'
    )
    ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
), orc_winter_grazing AS (
    SELECT DISTINCT ON (uop_h3.h3_index)
        uop_h3.h3_index,
        c.source_date,
        c.source_data,
        c.source_scale,
        TRUE AS intensive,
        ARRAY[]::TEXT[] AS commod,
        ARRAY['winter grazing']::TEXT[] AS manage
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
    AND c.activity_tsvector @@ phraseto_tsquery('english', 'intensive winter grazing')
    ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
), orc_dairy_pastoral AS (
    SELECT DISTINCT ON (uop_h3.h3_index)
        uop_h3.h3_index,
        c.source_date,
        c.source_data,
        c.source_scale,
        FALSE AS intensive,
        ARRAY['cattle dairy']::TEXT[] AS commod,
        NULL::TEXT[] AS manage
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
), trc_pastoral AS (
    SELECT DISTINCT ON (uop_h3.h3_index)
        uop_h3.h3_index,
        c.source_date,
        c.source_data,
        c.source_scale,
        FALSE AS intensive,
        ARRAY[]::TEXT[] AS commod,
        NULL::TEXT[] AS manage
    FROM trc_discharge_permits c
    JOIN trc_discharge_permits_h3 c_h3 ON c.ogc_fid = c_h3.ogc_fid
    JOIN unit_of_property_h3 uop_inner_h3 ON c_h3.h3_index && uop_inner_h3.h3_index
    JOIN unit_of_property uop_inner ON uop_inner_h3.ogc_fid = uop_inner.ogc_fid
        AND ST_Within(c.geom, uop_inner.geom)
    JOIN unit_of_property uop_all ON uop_inner.unit_of_property_id = uop_all.unit_of_property_id
    JOIN unit_of_property_h3 uop_h3 ON uop_all.ogc_fid = uop_h3.ogc_fid
    WHERE :parent::h3index = c_h3.h3_partition
    AND   :parent::h3index = uop_inner_h3.h3_partition
    AND   :parent::h3index = uop_h3.h3_partition
    AND c.Status = 'Current'
    AND c.PrimaryIndustryPurpose = 'Agriculture'
    AND c.Activity_Subtype IN ('Land - Animal Waste', 'Land/Water - Animal Waste', 'Water - Animal Waste')
    ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
), wcrc_shapes_pastoral AS (
    SELECT DISTINCT ON (h3_index)
        h3_index,
        source_date,
        source_data,
        source_scale,
        FALSE AS intensive,
        ARRAY['cattle dairy']::TEXT[] AS commod,
        NULL::TEXT[] AS manage
    FROM wcrc_consent_shapes
    JOIN wcrc_consent_shapes_h3 USING (ogc_fid)
    WHERE :parent::h3index = h3_partition
    AND CurrentStatus = 'Current'
    AND PrimaryIndustry = 'A0130 Dairy Cattle Farming'
    ORDER BY h3_index, upper(source_date) DESC NULLS LAST
), wcrc_points_pastoral AS (
    SELECT DISTINCT ON (uop_h3.h3_index)
        uop_h3.h3_index,
        c.source_date,
        c.source_data,
        c.source_scale,
        FALSE AS intensive,
        ARRAY['cattle dairy']::TEXT[] AS commod,
        NULL::TEXT[] AS manage
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
)
SELECT * FROM hbrc_pastoral
UNION ALL
SELECT * FROM horizons_pastoral
UNION ALL
SELECT * FROM horizons_pastoral_point
UNION ALL
SELECT * FROM wrc_pastoral
UNION ALL
SELECT * FROM orc_winter_grazing
UNION ALL
SELECT * FROM orc_dairy_pastoral
UNION ALL
SELECT * FROM trc_pastoral
UNION ALL
SELECT * FROM wcrc_shapes_pastoral
UNION ALL
SELECT * FROM wcrc_points_pastoral;
