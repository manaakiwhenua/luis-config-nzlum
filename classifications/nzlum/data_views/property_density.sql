-- Count distinct unit_of_property records whose H3 cells overlap each ROI cell.
-- A count >= 2 indicates multiple small properties fitting within one H3 cell,
-- which is a direct spatial density signal independent of DVR coding.
CREATE TEMPORARY VIEW property_density_ AS
SELECT roi.h3_index,
    COUNT(DISTINCT uop_h3.ogc_fid) AS overlapping_properties
FROM roi
JOIN unit_of_property_h3 uop_h3 ON roi.h3_index && uop_h3.h3_index
WHERE :parent::h3index = uop_h3.h3_partition
GROUP BY roi.h3_index;
