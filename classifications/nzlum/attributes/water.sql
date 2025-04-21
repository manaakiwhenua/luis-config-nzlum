CREATE TEMPORARY VIEW water_features AS (
    -- TODO record scale of water featuere? 1:50000
    SELECT * FROM (
        SELECT h3_index,
        COALESCE(
            rivers.feature,
            rivers_polylines.feature,
            ecan_braided_rivers_.water,

            fenz_lakes_.water,
            lakes.feature,

            lagoons.feature,

            ponds.feature,

            swamps.feature,

            mangroves.feature,

            drains.feature,

            canals.feature,

            ice.feature,

            mud.feature,

            CASE
                WHEN (
                    land.h3_index IS NULL
                    AND sand.h3_index IS NULL
                ) THEN 'marine'
                ELSE NULL
            END
        ) AS feature
        FROM roi
        LEFT JOIN (
            SELECT h3_index, 'river' AS feature
            FROM topo50_river_pol
            RIGHT JOIN topo50_river_pol_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
            UNION ALL
            SELECT h3_index, 'river' AS feature
            FROM topo50_chatham_river_pol
            RIGHT JOIN topo50_chatham_river_pol_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) rivers
        USING (h3_index)
        LEFT JOIN (
            SELECT h3_index, 'river' AS feature
            FROM topo50_river
            RIGHT JOIN topo50_river_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
            UNION ALL
            SELECT h3_index, 'river' AS feature
            FROM topo50_chatham_river
            RIGHT JOIN topo50_chatham_river_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) rivers_polylines
        USING (h3_index)
        LEFT JOIN (
            SELECT h3_index, 'lake' AS feature
            FROM topo50_lake
            RIGHT JOIN topo50_lake_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
            UNION ALL
            SELECT h3_index, 'lake' AS feature
            FROM topo50_chatham_lake
            RIGHT JOIN topo50_chatham_lake_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) lakes -- NB lake_use records 'hydro-electric' and 'reservoir'
        USING (h3_index)
        LEFT JOIN (
            SELECT h3_index, 'lagoon' AS feature
            FROM topo50_lagoon
            RIGHT JOIN topo50_lagoon_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
            UNION ALL
            SELECT h3_index, 'lagoon' AS feature
            FROM topo50_chatham_lagoon
            RIGHT JOIN topo50_chatham_lagoon_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) lagoons
        USING (h3_index)
        LEFT JOIN (
            SELECT h3_index, 'pond' AS feature
            FROM topo50_pond
            RIGHT JOIN topo50_pond_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) ponds
        USING (h3_index)
        LEFT JOIN (
            SELECT h3_index, 'swamp' AS feature
            FROM topo50_swamp
            RIGHT JOIN topo50_swamp_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
            UNION ALL
            SELECT h3_index, 'swamp' AS feature
            FROM topo50_chatham_swamp
            RIGHT JOIN topo50_chatham_swamp_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) swamps
        USING (h3_index)
        LEFT JOIN (
            SELECT h3_index, 'mangrove' AS feature
            FROM topo50_mangrove
            RIGHT JOIN topo50_mangrove_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) mangroves
        USING (h3_index)
        LEFT JOIN (
            SELECT h3_index, 'drain' AS feature
            FROM topo50_drain
            RIGHT JOIN topo50_drain_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
            UNION ALL
            SELECT h3_index, 'drain' AS feature
            FROM topo50_chatham_drain
            RIGHT JOIN topo50_chatham_drain_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) drains
        USING (h3_index)
        LEFT JOIN (
            SELECT h3_index, 'canal' AS feature
            FROM topo50_canal
            RIGHT JOIN topo50_canal_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) canals
        USING (h3_index)
        LEFT JOIN (
            SELECT h3_index, 'ice' AS feature
            FROM topo50_ice
            RIGHT JOIN topo50_ice_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) ice
        USING (h3_index)
        LEFT JOIN (
            SELECT h3_index, 'mud' AS feature
            FROM topo50_mud
            RIGHT JOIN topo50_mud_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
            UNION ALL
            SELECT h3_index, 'mud' AS feature
            FROM topo50_chatham_mud
            RIGHT JOIN topo50_chatham_mud_h3 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) mud
        USING (h3_index)
        LEFT JOIN (
            SELECT h3_index
            FROM topo50_land_h3
            WHERE :parent::h3index = h3_partition
        ) AS land
        USING (h3_index)
        LEFT JOIN (
            SELECT h3_index
            FROM topo50_sand_h3
            WHERE :parent::h3index = h3_partition
            UNION ALL
            SELECT h3_index
            FROM topo50_chatham_sand_h3
            WHERE :parent::h3index = h3_partition
        ) AS sand
        USING (h3_index)
        LEFT JOIN (
            SELECT h3_index, water
            FROM ecan_braided_rivers_h3
            JOIN ecan_braided_rivers USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) AS ecan_braided_rivers_
        USING (h3_index)
        LEFT JOIN (
            SELECT h3_index, water
            FROM fenz_lakes_h3
            JOIN fenz_lakes USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) AS fenz_lakes_
        USING (h3_index)
    -- TODO topo50_river (i.e. centrelines),,
    -- TODO estuary?
    -- TODO hydro parcels?
    )
    WHERE feature IS NOT NULL
);