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
        Authorisation_Decision_Date DESC NULLS LAST, -- Most recently issued
        Authorisation_Expiry_Date DESC NULLS LAST, -- Latest expiry
        Area_m2 DESC NULLS LAST, -- Largest
        Document_Link DESC NULLS LAST -- Prefer with more information
),
forestry_horizons AS (
    SELECT
        DISTINCT ON (h3_index)
        h3_index,
        source_date,
        source_data,
        source_scale,
        CASE
            WHEN PRIMARY_PURPOSE ~ '\mAfforestation\M'
            THEN TRUE
            ELSE FALSE
        END AS afforestation_flag
        -- PRIMARY_INDUSTRY,
        -- PRIMARY_PURPOSE
    FROM horizons_reg_act
    INNER JOIN horizons_reg_act_h3 USING (ogc_fid)
    WHERE :parent::h3index = h3_partition
    AND STATUS = 'Current'
    AND PRIMARY_INDUSTRY = 'Forestry'
    AND (
        PRIMARY_PURPOSE IS NULL
        OR (
            PRIMARY_PURPOSE NOT LIKE 'Construction | Access Road %'
            AND PRIMARY_PURPOSE NOT LIKE 'Manufacturing %'
            AND PRIMARY_PURPOSE NOT LIKE 'Mining %'
        )
    )
    ORDER BY
        h3_index,
        COMMENCEMENT_DATE DESC NULLS LAST, -- Most recently issued
        EXPIRY_DATE DESC NULLS LAST, -- Latest expiry
        Shape_Area DESC NULLS LAST -- Largest
),
forestry_horizons_point AS (
    SELECT DISTINCT ON (uop_h3.h3_index)
        uop_h3.h3_index,
        c.source_date,
        c.source_data,
        c.source_scale,
        CASE
            WHEN c.PRIMARY_PURPOSE ~ '\mAfforestation\M'
            THEN TRUE
            ELSE FALSE
        END AS afforestation_flag
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
    AND c.PRIMARY_INDUSTRY = 'Forestry'
    AND (
        c.PRIMARY_PURPOSE IS NULL
        OR (
            c.PRIMARY_PURPOSE NOT LIKE 'Construction | Access Road %'
            AND c.PRIMARY_PURPOSE NOT LIKE 'Manufacturing %'
            AND c.PRIMARY_PURPOSE NOT LIKE 'Mining %'
        )
    )
    ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
),
forestry_wrc AS (
    SELECT DISTINCT ON (uop_h3.h3_index)
        uop_h3.h3_index,
        c.source_date,
        c.source_data,
        c.source_scale,
        (
            c.DESCRIPTIVE_TEXT ~* '\bafforestation\b'
            OR c.ACTIVITY_SUBTYPE ~* '\bafforestation\b'
        ) AS afforestation_flag
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
    AND c.PRIMARY_INDUSTRY_PURPOSE = 'Forestry'
    ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
),
forestry_wrc_discharge AS (
    SELECT DISTINCT ON (uop_h3.h3_index)
        uop_h3.h3_index,
        c.source_date,
        c.source_data,
        c.source_scale,
        (
            c.DESCRIPTIVE_TEXT ~* '\bafforestation\b'
            OR c.ACTIVITY_SUBTYPE ~* '\bafforestation\b'
        ) AS afforestation_flag
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
    AND c.PRIMARY_INDUSTRY_PURPOSE = 'Forestry'
    ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
),
trc_afforestation AS (
    SELECT DISTINCT ON (uop_h3.h3_index)
        uop_h3.h3_index,
        c.source_date,
        c.source_data,
        c.source_scale,
        TRUE AS afforestation_flag
    FROM trc_land_use_consents c
    JOIN trc_land_use_consents_h3 c_h3 ON c.ogc_fid = c_h3.ogc_fid
    JOIN unit_of_property_h3 uop_inner_h3 ON c_h3.h3_index && uop_inner_h3.h3_index
    JOIN unit_of_property uop_inner ON uop_inner_h3.ogc_fid = uop_inner.ogc_fid
        AND ST_Within(c.geom, uop_inner.geom)
    JOIN unit_of_property uop_all ON uop_inner.unit_of_property_id = uop_all.unit_of_property_id
    JOIN unit_of_property_h3 uop_h3 ON uop_all.ogc_fid = uop_h3.ogc_fid
    WHERE :parent::h3index = c_h3.h3_partition
    AND   :parent::h3index = uop_inner_h3.h3_partition
    AND   :parent::h3index = uop_h3.h3_partition
    AND c.Status = 'Current'
    AND c.Activity_Subtype = 'Forestry – Afforestation'
    ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
),
wcrc_points_forestry AS (
    SELECT DISTINCT ON (uop_h3.h3_index)
        uop_h3.h3_index,
        c.source_date,
        c.source_data,
        c.source_scale,
        FALSE AS afforestation_flag
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
    AND c.PrimaryIndustry = 'A0301 Forestry'
    ORDER BY uop_h3.h3_index, upper(c.source_date) DESC NULLS LAST
)
SELECT * FROM forestry_hbrc
UNION ALL
SELECT * FROM forestry_horizons
UNION ALL
SELECT * FROM forestry_horizons_point
UNION ALL
SELECT * FROM forestry_wrc
UNION ALL
SELECT * FROM forestry_wrc_discharge
UNION ALL
SELECT * FROM trc_afforestation
UNION ALL
SELECT * FROM wcrc_points_forestry;

-- TODO ActPrimaryPurpose = 'Forestry - Afforestation' OR 'Mechanical Land Preparation' (transitionary class?)
-- TODO harvesting consent = commodity = {sawlogs,pulpwood}?