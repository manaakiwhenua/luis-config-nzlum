CREATE TEMPORARY VIEW marine_farms AS (
    WITH mpi_farms AS (
        SELECT DISTINCT ON (h3_index) h3_index,
        ts_species_group,
        daterange(
            '2007-03-01'::DATE,
            '2007-03-31'::DATE,
            '[]'
        ) AS source_date,
        '[30,100)'::int4range AS source_scale,
        'MPI' AS source_data
        FROM mpi_current_marine_farms_h3
        INNER JOIN mpi_current_marine_farms USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND EXISTS (SELECT 1 FROM roi WHERE roi.h3_index && mpi_current_marine_farms_h3.h3_index)
        ORDER BY
            h3_index,
            coastal_permit_expiry_date DESC NULLS LAST,
            Effective_date DESC NULLS LAST,
            total_size_ha ASC NULLS LAST
    ),
    topo50_farms AS (
        SELECT h3_index,
        species,
        daterange(
            '2011-05-22'::DATE,
            '2025-01-02'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_marine_farms
        INNER JOIN topo50_marine_farms_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND EXISTS (SELECT 1 FROM roi WHERE roi.h3_index && topo50_marine_farms_h3.h3_index)
    ),
    topo50_farms_terrestrial AS (
        SELECT h3_index,
        species,
        daterange(
            '2011-05-22'::DATE,
            '2025-04-28'::DATE,
            '[]'
        ) AS source_date,
        '[60,100)'::int4range AS source_scale,
        'LINZ' AS source_data
        FROM topo50_fish_farms
        INNER JOIN topo50_fish_farms_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND EXISTS (SELECT 1 FROM roi WHERE roi.h3_index && topo50_fish_farms_h3.h3_index)
    )
    SELECT DISTINCT ON (roi.h3_index)
        roi.h3_index,
        CASE
            WHEN mpi_farms.h3_index IS NOT NULL AND topo50_farms.h3_index IS NOT NULL
            THEN 1
            WHEN topo50_farms_terrestrial.h3_index IS NOT NULL
            THEN 1
            WHEN mpi_farms.h3_index IS NOT NULL OR topo50_farms.h3_index IS NOT NULL
            THEN 3
            ELSE NULL
        END AS confidence,
        range_merge(datemultirange(
            VARIADIC ARRAY_REMOVE(ARRAY[
                mpi_farms.source_date,
                topo50_farms.source_date,
                topo50_farms_terrestrial.source_date
            ], NULL)
        ))::daterange AS source_date,
        range_merge(int4multirange(
            VARIADIC ARRAY_REMOVE(ARRAY[
                mpi_farms.source_scale,
                topo50_farms.source_scale,
                topo50_farms_terrestrial.source_scale
            ], NULL)
        ))::int4range AS source_scale,
        ARRAY_REMOVE(ARRAY[
            mpi_farms.source_data,
            topo50_farms.source_data,
            topo50_farms_terrestrial.source_data
        ], NULL)::TEXT[] AS source_data,
        CASE
            WHEN mpi_farms.ts_species_group @@ to_tsquery('finfish & (shellfish | crustaean)')
            THEN ARRAY['finfish','molluscs']::TEXT[]
            WHEN mpi_farms.ts_species_group @@ to_tsquery('finfish')
            THEN ARRAY['finfish']::TEXT[]
            WHEN mpi_farms.ts_species_group @@ to_tsquery('shellfish | crustacean') OR topo50_farms.species = 'mussels'
            THEN ARRAY['molluscs']::TEXT[]
            WHEN topo50_farms_terrestrial.species = 'salmon'
            THEN ARRAY['finfish']::TEXT[]
            ELSE NULL
        END AS commod,
        ARRAY[]::TEXT[] AS manage
    FROM roi
    LEFT JOIN mpi_farms ON roi.h3_index && mpi_farms.h3_index
    LEFT JOIN topo50_farms ON roi.h3_index && topo50_farms.h3_index
    LEFT JOIN topo50_farms_terrestrial ON roi.h3_index && topo50_farms_terrestrial.h3_index
    WHERE mpi_farms.h3_index IS NOT NULL
       OR topo50_farms.h3_index IS NOT NULL
       OR topo50_farms_terrestrial.h3_index IS NOT NULL
    ORDER BY roi.h3_index
);
