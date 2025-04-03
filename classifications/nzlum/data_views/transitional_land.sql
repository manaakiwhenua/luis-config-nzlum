CREATE TEMPORARY VIEW transitional_land AS (
    SELECT h3_index, source_data, source_date, source_scale FROM (
        SELECT
            DISTINCT ON (h3_index) *,
            daterange(
                '2023-09-30'::DATE,
                '2025-02-11'::DATE,
                '[]'
            ) AS source_date,
            'HBRC'::TEXT AS source_data,
        '(1,100)'::int4range AS source_scale --  -- Unstated precision, assume parcel scale equivalent
        FROM hawkes_bay_land_categorisation_h3
        INNER JOIN hawkes_bay_land_categorisation USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND category = '3' -- See https://www.hastingsdc.govt.nz/land-categorisation-hb/land-categorisation-maps/
        ORDER BY
            h3_index,
            category DESC NULLS LAST
    ) UNION ALL
    SELECT h3_index, source_data, source_date, source_scale FROM (
        SELECT
            DISTINCT ON (h3_index) *,
            daterange(
                '2016-08-24'::DATE,
                '2021-08-01'::DATE,
                '[]'
            ) AS source_date,
            'CERA'::TEXT AS source_data,
        '(1,100)'::int4range AS source_scale -- Unstated precision, assume parcel scale equivalent
        FROM cera_red_zoned_land_h3
        WHERE :parent::h3index = h3_partition
        ORDER BY
            h3_index
    ) UNION ALL
    SELECT h3_index, source_data, source_date, source_scale FROM (
        SELECT
            DISTINCT ON (h3_index) *,
            daterange(
                '2023-06-16'::DATE,
                '2024-06-19'::DATE,
                '[]'
            ) AS source_date,
            'GDC'::TEXT AS source_data,
        '(1,100)'::int4range AS source_scale -- Unstated precision, assume parcel scale equivalent
        FROM fosal_areas_tairawhiti
        JOIN fosal_areas_tairawhiti_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND category IN (
            '2A', -- Significant further assessment is required to assess a property, could move to any of the other categories 1-3.
            '2C', -- Community level interventions are needed for managing future severe weather risk events
            '2P', -- Property level interventions needed, e.g. drainage, raising houses
            '3' -- Future risk cannot be mitigated
        )
    )
);