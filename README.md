# NZLUM Configuration

Configuration files for [LUIS](https://github.com/manaakiwhenua/luis) for the [NZLUM classification](https://github.com/manaakiwhenua/nzsluc/tree/main/classification-systems/nzlum).

> [!NOTE]
> The Land Use Information System (LUIS) is proprietary software, but relies on a structured set of configuration files, which are available here for this particular classification system (the New Zealand Land Use and Management classification, NZLUM). This README loosely describes this structure, but this repository contains nothing more than the authoritative configuration files and does not include any software. It is used a submodule for LUIS.

## Structure

### [`profiles/nzlum/config.yaml`](profiles/nzlum/config.yaml)

This sets run-time parameters for Snakemake (used by LUIS), and importantly declares which other YAML files together constitute the configuration for the classification with the `configfiles` attribute. These files are ultimately merged into one large configuration, and as later files can override settings from earlier files, the `configfiles` directive determines this order.

### `external-data/**/*.yml`

`external-data` is a top-level property in the configuration YAML where "external data" is declared. That is principally: what it is, license, attribution, how it is made available. In addition, various transformations can be declared to effect data on import, including the creation of appropriate database indices, new derived columns, and filtering clauses to exclude data from these sources.

Heavy use of YAML templating is applied as many datasets come from shared collections with the same license, API, etc.

There is support for configuring access to local files, remote files over HTTP, WFS, and ArcGIS REST API. Data can be spatial or non-spatial (e.g. attribute tables with join keys to other tables).

Example, from [`external-data/landcare/landcare.yml`](external-data/landcare/landcare.yml):

```yaml
__publisher_lcr: &lcr
  license: https://creativecommons.org/licenses/by/4.0/
  attribution: "Sourced from the Land Resource Information Systems (LRIS) Portal and licensed for reuse under CC BY 4.0"

__publisher_lcr_wfs: &lcr_wfs
  <<: *lcr
  host: https://lris.scinfo.org.nz/services;key={key}/wfs
  key: LRIS_KEY
  external_data_type: wfs

external_data:

  lcdb_v5:
    <<: *lcr_wfs
    description: LCDB v5
    layer: layer-104400
    geom_type: multipolygon
    indices: &lcdb_indices
      - columns: [Name_2018]
        type: btree
        name: idx_name_2018
      - columns: [Class_2018]
        type: btree
        name: idx_class_2018
      - columns: [editdate]
        type: btree
        name: idx_editdate
      - columns: [geom]
        type: gist
        name: idx_gist_geom


  lcdb_v5_chathams:
    <<: *lcr_wfs
    description: LCDB v5 [Chatham Islands]
    layer: layer-104442
    geom_type: multipolygon
    indices: *lcdb_indices
```

This example demonstrates how LCDB v5 is configured. It uses YAML templates for shared properties across LCDB mainland and Chatham Island versions, which have the same schema and can therefore also share the same `indices`. Indices include a GiST index for geometry data, and three btree indices for the `Name_2018`, `Class_2018`, and `editdate` attributes as they are used within later queries (which perform the land use classification).

> [!IMPORTANT]
> The `key` property should refer to an environment variable which will be checked at runtime. Unless a key is supposed to be public, it is never to be included in configuration beyond the expected name of the environment variable. It can be included in any part of the API URL as above by using `{key}` as above.


### [`classifications/nzlum`](classifications/nzlum)

The `classifications` directory is for logic relating more to the classification implementation, as distinct from configuration describing what and how to access published data.

> [!NOTE]
> As more versions of the NZLUM classification are published, additional classifications may be added to this directory so that older versions remain available for inspection and re-use; for now it only contains the configuration for the pilot implementation of the NZLUM classification.

[`classifications/nzlum/config.nzlum.yml`](classifications/nzlum/config.nzlum.yml)

This configuration files adds a few more top-level attributes:

- `region_of_interest.id`: place to specify an ID for a layer (already described in `external_data`) that determines the geographical extent of the classification.
- `h3.resolution`: the [H3 DGGS resolution](https://h3geo.org/docs/core-library/restable/) at which the classification is to be made.
- `classifications.nzlum`
    - `depends`: array of layer IDs (defined in `external_data`) on which the classification depends (and therefore should be downloaded and processed as described).
    - `table_schema`: the classification produces an output with attributes and these attributes have particular types, together these are a data schema. For example, `lu_code_primary: INTEGER`.
    - `outputs.nzlum`: declares output geospatial data format, and the location of SQL files where the combinatorial logic is written.
        - `preclassify`: A SQL file (PostgreSQL dialect) to be executed _before_ the classification proper. Can be used to declare new types, useful functions, etc. See [nzlum-preclassify.sql](classifications/nzlum/nzlum-preclassify.sql). Fucntions may be written using PL/pgSQL.
        - `query`: A SQL file (PostgreSQL dialect) that performs the classification itself. Using [psql](https://www.postgresql.org/docs/current/app-psql.html) this can make use of `\ir filename` (`\include_relative filename`), additional SQL files can be included to make the SQL file more maintainable. There are two primary constraints:
            - The query must work on the basis of H3 partitions, referred to by using the `:parent` variable (which will be substituted for the parent H3 index).
            - The output of the query must match the `table_schema` as described in the configuration.

For maintainability, the primary query is [nzlum.sql](classifications/nzlum/nzlum.sql) but this depends on a number of `\include_relative` files which are also SQL.

- [`classifications/nzlum/data_views`](classifications/nzlum/data_views) includes abstract views of data. These are used e.g. to combine datasets that are not published together but can easily be (see [`classifications/nzlum/data_views/lcdb.sql`](classifications/nzlum/data_views/lcdb.sql)), or for datasets that essentially describe the same thing but take more effort to amalgamate (see [classifications/nzlum/data_views/consents/forestry.sql](classifications/nzlum/data_views/consents/forestry.sql)). The intention here is to allow transformation of input datasets to happen dynamically at runtime, and to hide this transformation behind the common interface of a temporary table view.

Example [classifications/nzlum/data_views/transitional_land.sql](classifications/nzlum/data_views/transitional_land.sql), for assembling a collection of candidates for transitional land, which may be referred to by more than one class:

```sql
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
```

This demonstrates how the CERA Red Zone Land (Christchurch), the Hawke's Bay land categorisation (post-Cyclone Gabrielle), and the future of severely affected land (FOSAL) areas for the Tarāwhiti/Gisborne region (Gabrielle) are combined into one abstraction that reduces the complexity of the inputs while retaining pertinent variable information from each (`source_date`, etc.) Classes that need to consider such land can then refer to the `transitional_land` view without needing to redetermine how this data is best combined. If more transitional land needs to be included, it is likely that only this view needs to be revised, and _not_ the SQL definition of a class, which will automatically "receive" changes to this definition.


- [`classifications/nzlum/classes`](classifications/nzlum/classes). NZLUM is here implemented as a series of distinct classifications for each class. Each DGGS cell may be independently classified as each class. But alongside the identification with a class, is a confidence value, a geographic scale of the input data, and a date interval for input data. These are leveraged in a `SORT BY` clause that ultimately gives each location a distinct identification with a particular class (preferring more confident, more recent, and more spatially precise identification).

Example [classifications/nzlum/classes/class_136.sql](classifications/nzlum/classes/class_136.sql), for class `1.3.6`, Defence land:

```sql
CREATE TEMPORARY VIEW class_136 AS ( 
    SELECT h3_index,
    1 AS lu_code_primary,
    3 AS lu_code_secondary,
    6 AS lu_code_tertiary,
    CASE
        WHEN (
            linz_crosl_nzdf.h3_index IS NOT NULL
            AND linz_dvr_.actual_property_use = '45' -- Defence
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[
                linz_crosl_nzdf.source_data,
                linz_dvr_.source_data
            ]::TEXT[],
            range_merge(
                linz_crosl_nzdf.source_date,
                linz_dvr_.source_date
            )::daterange,
            range_merge(
                linz_crosl_nzdf.source_scale,
                linz_dvr_.source_scale
            )::int4range
        )::nzlum_type
        WHEN linz_dvr_.actual_property_use = '45' -- Defence
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_.source_data]::TEXT[],
            linz_dvr_.source_date,
            linz_dvr_.source_scale
        )::nzlum_type
        WHEN linz_crosl_nzdf.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN linz_dvr_.actual_property_use IN ('04', '40') -- Mixed use, including community services (which itself includes defence)
                THEN 1
                WHEN linz_dvr_.actual_property_use ~ '^5' -- Recreational
                THEN 2
                ELSE 12
            END,
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY_REMOVE(ARRAY[linz_crosl_nzdf.source_data, linz_dvr_.source_data], NULL)::TEXT[],
            range_merge(datemultirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    linz_crosl_nzdf.source_date,
                    linz_dvr_.source_date
                ], NULL)
            ))::daterange, -- source_date
            range_merge(int4multirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    linz_crosl_nzdf.source_scale,
                    linz_dvr_.source_scale
                ], NULL)
            ))::int4range -- source_scale
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM (
        SELECT *
        FROM linz_crosl_
        WHERE managed_by = 'New Zealand Defence Force'
    ) AS linz_crosl_nzdf
    FULL OUTER JOIN linz_dvr_ USING (h3_index)
    INNER JOIN ( -- Exclude built-up areas from this class
        SELECT * 
        FROM lcdb_
        WHERE Class_2018 NOT IN (
            1, -- 'Built-up Area (settlement)',
            2, -- 'Urban Parkland/Open Space'
            5 --'Transport Infrastructure',
        )
    ) AS lcdb_built_up USING (h3_index)
);
```

This defines a temporary view (limited to one H3 partition) that uses the Central Record of State Land (CRoSL) for the identification of NZDF managed land, but confirmed against actual use as recorded in the District Valuation Roll (DVR) where possible, in order to exclude from the CRoSL set NZDF-managed land that is residential, and offer a lower-confidence classification where the DVR indicates that the land is "passive recreational".

Independently of CRoSL, the DVR also has an identification of land as being for defence purposes (`actual_property_use = 45`). But since there is also an `INNER JOIN ` with a subset of LCDB, any land that is _also_ part of a built-up area, urban parkland or transport infrastructure, is _not_ included for consideration. For instance, this excludes Ōhakea Airbase for consideration as class 1.3.6, as it is not a natural area—whereas the Waiouru New Zealand Army training camp _is_ still included.