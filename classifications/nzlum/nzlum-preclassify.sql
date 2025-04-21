-- Hash a text array into a 64 bit signed integer (int64).
-- As a simple subset of the MD5 hash, it is more likely to have hash collisions than MD5,
--  but this is still unlikely.
CREATE OR REPLACE FUNCTION hash_bigint(text_array text[]) RETURNS bigint
    AS \$\$
        SELECT (('x'||substr(md5(array_to_string(text_array,'*')),1,8))::bit(32)::integer + 2^31)::bigint;
    \$\$
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;

DROP TYPE IF EXISTS nzlum_type CASCADE;
CREATE TYPE nzlum_type AS (
    lu_code_ancillary TEXT[],
    confidence INTEGER,
    commod TEXT[], -- TODO controlled vocab
    manage TEXT[],  -- TODO controlled vocab
    source_data TEXT[],
    source_date DATERANGE, -- For evergreen data with no information, recommend [CURRENT_DATE,CURRENT_DATE], rather than NULL or [CURRENT_DATE,) unless there is good reason to do so
    source_scale INT4RANGE -- precision in metres
);

DROP TYPE IF EXISTS nzlum0_lu_coden CASCADE;
CREATE TYPE nzlum0_lu_coden AS ENUM (
    -- Classification confusion should be first avoided by considering confidence,
    --      source_scale, source_date, and perhaps source_data.
    --      But where there is still confusion, this enumeration of classes can be used
    --      to resolve it, by using it as a sort order.
    -- It can also be used as a type to enusre that no invalid classes are included
    --      in error.

    '1.1.1',
    '1.1.2',
    '1.1.3',
    '1.1.4',
    '1.1.5',
    '1.1.6',
    '1.1.7',
    '1.1.0',
    '1.2.0',
    '1.3.6',
    '1.3.0',
    '1.4.0',

    '3.6.0', -- Transport overays (road, etc)

    '2.7.0', -- Prefer bringing forward water related features; NB this class is probably better left to water attribute 
    '2.1.0',
    '2.2.0',
    '2.3.0',
    '2.4.0',
    '2.5.0',
    '2.6.0',
    '2.8.0',

    '3.1.0',
    '3.2.0',
    '3.3.0',
    '3.4.0',
    '3.5.0',
    '3.7.0',
    '3.8.0',
    '3.9.0',

    '0'
);

CREATE OR REPLACE FUNCTION nzsluc_v0_2_0_lu_description(lu_code_primary integer, lu_code_secondary integer, lu_code_tertiary integer) RETURNS text
    AS \$\$
        SELECT CASE

            WHEN lu_code_primary = 1
            THEN CASE
                WHEN lu_code_secondary = 1 THEN
                    CASE
                        WHEN lu_code_tertiary = 1
                        THEN 'Strict nature reserve'
                        WHEN lu_code_tertiary = 2
                        THEN 'Wilderness area'
                        WHEN lu_code_tertiary = 3
                        THEN 'National park'
                        WHEN lu_code_tertiary = 4
                        THEN 'Natural feature protection'
                        WHEN lu_code_tertiary = 5
                        THEN 'Habitat or species management area'
                        WHEN lu_code_tertiary = 6
                        THEN 'Protected landscape'
                        WHEN lu_code_tertiary = 7
                        THEN 'Other conserved area'
                        ELSE 'Nature conservation'
                    END
                WHEN lu_code_secondary = 2 THEN 'Cultural and natural heritage'
                WHEN lu_code_secondary = 3 THEN
                    CASE
                        WHEN lu_code_tertiary = 6
                        THEN 'Defence land'
                        ELSE 'Minimal use from relatively natural environments'
                    END
                WHEN lu_code_secondary = 4 THEN 'Unused land and land in transition'
                END
            
            WHEN lu_code_primary = 2
            THEN CASE
                WHEN lu_code_secondary = 1 THEN 'Plantation forests'
                WHEN lu_code_secondary = 2 THEN 'Grazing modified pasture systems'
                WHEN lu_code_secondary = 3 THEN 'Short-rotation and seasonal cropping'
                WHEN lu_code_secondary = 4 THEN 'Perennial horticulture'
                WHEN lu_code_secondary = 5 THEN 'Intensive horticulture'
                WHEN lu_code_secondary = 6 THEN 'Intensive animal production'
                WHEN lu_code_secondary = 7 THEN 'Water and wastewater'
                WHEN lu_code_secondary = 8 THEN 'Land in transition'
                END

            WHEN lu_code_primary = 3
            THEN CASE
                WHEN lu_code_secondary = 1 THEN 'Residential'
                WHEN lu_code_secondary = 2 THEN 'Public recreation and services'
                WHEN lu_code_secondary = 3 THEN 'Commercial'
                WHEN lu_code_secondary = 4 THEN 'Manufacturing and industrial'
                WHEN lu_code_secondary = 5 THEN 'Utilities'
                WHEN lu_code_secondary = 6 THEN 'Transport and communication'
                WHEN lu_code_secondary = 7 THEN 'Mining'
                WHEN lu_code_secondary = 8 THEN 'Waste treatment and disposal'
                WHEN lu_code_secondary = 9 THEN 'Vacant and transitioning land'
                END

            ELSE NULL
        END lu_description;
    \$\$
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;

-- CREATE OR REPLACE FUNCTION greatest_ignore_null(vals NUMERIC[])
-- RETURNS NUMERIC
--     AS \$\$
--     DECLARE
--     output NUMERIC := NULL;
--     val NUMERIC;
--     BEGIN
--     FOREACH val IN ARRAY vals LOOP
--         IF val IS NOT NULL AND (output IS NULL OR val > output) THEN
--         output := val;
--         END IF;
--     END LOOP;
--     RETURN output;
--     END;
--     \$\$
-- LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION earliest_date(p_input DATE[])
  RETURNS DATE AS \$\$
    SELECT *
    FROM unnest(p_input) AS x(v)
    ORDER BY x.v NULLS LAST
    LIMIT 1
\$\$
LANGUAGE SQL
IMMUTABLE;

CREATE OR REPLACE FUNCTION latest_date(p_input DATE[])
  RETURNS DATE AS \$\$
    SELECT *
    FROM unnest(p_input) AS x(v)
    ORDER BY x.v DESC NULLS LAST
    LIMIT 1
\$\$
LANGUAGE SQL
IMMUTABLE;

CREATE OR REPLACE FUNCTION convert_to_daterange(date_array DATE[])
RETURNS DATERANGE AS \$\$
DECLARE
    cleaned_array DATE[];
    array_size INT;
    dims INT;
BEGIN
    -- Log the input array
    -- RAISE NOTICE 'Input date array: %', date_array;
    dims := array_ndims(date_array);
    IF dims > 1 THEN
        RAISE EXCEPTION 'Input date array dimensions: %', dims;
    END IF;

    IF date_array IS NULL THEN
        RETURN NULL; -- Return NULL if the input array is NULL
    END IF;

    -- Remove NULL values and store the cleaned array
    RAISE NOTICE 'Dimensions of date_array before array_remove: %', array_ndims(date_array);
    cleaned_array := array_remove(date_array, NULL);
    RAISE NOTICE 'Dimensions of date_array after array_remove: %', array_ndims(date_array);
    -- RAISE NOTICE 'Cleaned array (after removing NULLs): %', cleaned_array;
    dims := array_ndims(cleaned_array);
    IF dims > 1 THEN
        RAISE EXCEPTION 'Input date array dimensions: %', dims;
    END IF;
    -- Get the length of the cleaned array
    array_size := array_length(cleaned_array, 1); -- array_length produces NULL instead of 0 for empty or missing array dimensions
    -- RAISE NOTICE 'Array size: %', array_size;
    

    IF array_size = 0 OR array_size IS NULL THEN
        RAISE NOTICE 'Returning NULL';
        RETURN NULL; -- Return NULL for empty arrays
    ELSIF array_size = 1 THEN
        RETURN daterange(cleaned_array[1], cleaned_array[1], '[]'); -- Single date, create range with same start and end date
    ELSE
        RETURN daterange(
            earliest_date(cleaned_array),
            latest_date(cleaned_array),
            '[]' -- Inclusive range
        );
    END IF;
END;
\$\$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION deduplicate_and_sort(arr TEXT[]) -- NB also filters out NULL
RETURNS TEXT[] AS \$\$
BEGIN
    RETURN ARRAY(
        SELECT DISTINCT x
        FROM unnest(arr) as x
        WHERE x IS NOT NULL
        ORDER BY x
    );
END;
\$\$ LANGUAGE plpgsql;

-- parse out 'Of', 'O', and 'And' and remove the Title Case
-- This isn't possible: regexp_replace(managed_by, '\m(Of|And|O)\M', lower('\1'), 'gi') AS managed_by
CREATE OR REPLACE FUNCTION lowercase_conjunctions(text_name TEXT)
RETURNS text AS \$\$
BEGIN
  RETURN regexp_replace(
           regexp_replace(
             regexp_replace(text_name, '\mOf\M', 'of', 'gi'),
             '\mAnd\M', 'and', 'gi'), 
           '\mO\M', 'o', 'gi');
END;
\$\$ LANGUAGE plpgsql IMMUTABLE;