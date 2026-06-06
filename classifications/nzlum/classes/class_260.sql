CREATE TEMPORARY VIEW class_260 AS ( -- Intensive animal production
    SELECT roi.h3_index,
    2 AS lu_code_primary,
    6 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN marine_farms.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            marine_farms.confidence,
            marine_farms.commod,
            marine_farms.manage,
            marine_farms.source_data,
            marine_farms.source_date,
            marine_farms.source_scale,
            NULL
        )::nzlum_type
        WHEN intensive_livestock_consents_.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[],
            4,
            intensive_livestock_consents_.commod,
            ARRAY[]::TEXT[],
            ARRAY[intensive_livestock_consents_.source_data]::TEXT[],
            intensive_livestock_consents_.source_date,
            intensive_livestock_consents_.source_scale,
            NULL
        )::nzlum_type
        WHEN (
            linz_dvr_.actual_property_use IN ('0', '00', '01', '1', '10', '16')
            AND (
                linz_dvr_.category ~ '^S[AHPSX]'
                OR linz_dvr_.category ~ '^PS'
            )
        )
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN linz_dvr_.category ~ '^SH' AND horse_training_properties_.h3_index IS NOT NULL
                THEN 2 -- Confirmed horse training property (topo50 track)
                WHEN (
                    linz_dvr_.improvements_description ~ '\m(ANML|ANIMAL)\s?(SHLTR|SHELTER)S?\M'
                    OR linz_dvr_.improvements_description ~ '\m(STABLE|KENNEL)S?\M'
                    OR linz_dvr_.improvements_description ~ '\m(EQUES|EQUESTRIAN)\s?(CTR|CENTER|CENTRE)?\M'
                    -- OR linz_dvr_.improvements_description ~ '\mDEER\M'
                    OR linz_dvr_.improvements_description ~ '\mPIGGERY\M'
                    OR linz_dvr_.improvements_description ~ '\m(BROIL(ER)?|FOWL)\s?(HSE|HOUSE)S?\M'
                    OR linz_dvr_.improvements_description ~ '\mPOULTRY\M'
                    OR linz_dvr_.improvements_description ~ '\mEGG\sLAYING\M'
                )
                THEN 3
                WHEN horse_training_properties_.h3_index IS NOT NULL
                THEN 4 -- Possible horse training property
                WHEN linz_dvr_.category ~ '[AB]$'
                THEN 4 -- Higher confidence if a more economically viable property
                WHEN linz_dvr_.category ~ '[C]$'
                THEN 5
                WHEN linz_dvr_.zone !~ '^(0X|1|3|6)'
                THEN 9
                ELSE 6
            END, -- confidence
            ARRAY[
                CASE
                    WHEN linz_dvr_.category ~ '^SH' OR linz_dvr_.improvements_description ~ '\m(STABLES?|((EQUES|EQUESTRIAN)\s?(CTR|CENTER|CENTRE)?))\M'
                    THEN 'horses'
                    WHEN linz_dvr_.improvements_description ~ '\mEGG\sLAYING\M'
                    THEN 'chickens eggs'
                    WHEN linz_dvr_.category ~ '^SP'
                        OR linz_dvr_.improvements_description ~ '\m(BROIL(ER)?|FOWL)\s?(HSE|HOUSE)S?\M'
                        OR linz_dvr_.improvements_description ~ '\mPOULTRY\M'
                    THEN 'chickens'
                    WHEN linz_dvr_.category ~ '^SS' OR linz_dvr_.improvements_description ~ '\mPIGGERY\M'
                    THEN 'pigs'
                    ELSE NULL
                END
            ]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[linz_dvr_.source_data, horse_training_properties_.source_data]::TEXT[],
            range_merge(datemultirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    linz_dvr_.source_date,
                    horse_training_properties_.source_date
                ], NULL)
            ))::daterange,
            range_merge(int4multirange(
                VARIADIC ARRAY_REMOVE(ARRAY[
                    linz_dvr_.source_scale,
                    horse_training_properties_.source_scale
                ], NULL)
            ))::int4range,
            NULLIF(CONCAT_WS(E'\n',
                CASE WHEN horse_training_properties_.h3_index IS NOT NULL
                     THEN 'Horse training track' END,
                CASE WHEN linz_dvr_.improvements_description ~ '\m(STABLES?)\M'
                     THEN 'Stables' END,
                CASE WHEN linz_dvr_.improvements_description ~ '\m(EQUES|EQUESTRIAN)\s?(CTR|CENTER|CENTRE)?\M'
                     THEN 'Equestrian centre' END
            ), '') -- comment
        )::nzlum_type
        ELSE NULL
    END AS nzlum_type
    FROM roi
    LEFT JOIN marine_farms ON roi.h3_index && marine_farms.h3_index
    LEFT JOIN (
        SELECT h3_index, source_data, source_date, source_scale, commod
        FROM intensive_livestock_consents
    ) AS intensive_livestock_consents_ ON roi.h3_index && intensive_livestock_consents_.h3_index
    LEFT JOIN (
        SELECT
            h3_index,
            source_data,
            source_date,
            source_scale,
            improvements_description,
            actual_property_use,
            category,
            zone
        FROM linz_dvr_
        WHERE category ~ '^PS'
        OR category ~ '^S(A|H|P|S|X)' -- Specialist types except deer: aquaculture, horse studs and training operations, poultry, pigs, all other specialist livestock
        OR actual_property_use IN ('0', '00', '01', '1', '10', '16')
    ) linz_dvr_ ON roi.h3_index && linz_dvr_.h3_index
    LEFT JOIN (
        SELECT h3_index, source_data, source_date, source_scale
        FROM horse_training_properties_
    ) AS horse_training_properties_ ON roi.h3_index && horse_training_properties_.h3_index
    WHERE marine_farms.h3_index IS NOT NULL
       OR intensive_livestock_consents_.h3_index IS NOT NULL
       OR linz_dvr_.h3_index IS NOT NULL
);