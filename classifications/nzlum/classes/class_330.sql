CREATE TEMPORARY VIEW class_330 AS ( -- Commercial: retail, office, hospitality, entertainment, (private) healthcare, transportation, warehousing
    SELECT h3_index,
    3 AS lu_code_primary,
    3 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    CASE
        WHEN private_hospitals.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            1, -- confidence
            ARRAY[]::TEXT[], -- commod
            ARRAY[]::TEXT[], -- manage
            ARRAY[private_hospitals.source_data]::TEXT[],
            private_hospitals.source_date,
            private_hospitals.source_scale
        )::nzlum_type
        WHEN dvr_commercial.h3_index IS NOT NULL
        THEN ROW(
            ARRAY[]::TEXT[], -- lu_code_ancillary
            CASE
                WHEN
                    dvr_commercial.actual_property_use NOT IN (
                        '08',  -- Multi-use within commercial
                        '80', -- Multi-use within commercial
                        '78' -- Industrial, depots and yards
                    )
                    AND dvr_commercial.improvements_value > 0
                THEN 1
                WHEN 
                    dvr_commercial.actual_property_use IN (
                        '08',  -- Multi-use within commercial
                        '78' -- Industrial, depots and yards
                    )
                THEN CASE
                    WHEN dvr_commercial.improvements_description ~ '\m(SHOPS?|RESTAURANT|OFFICE|PROF\sOFFS?|TRUCK\s?STOP|SUPER\s?(MARKET|MKT|MARKE)|(SERV|SERVICE|S/)\s?(STN|STATION)|SALE\s?YARDS?|LIQUOR\s?STR|TAVERN|RETAIL|HOTEL|MOTEL|BAR|SHOW\s?ROOMS?|CINEMA|ACCOM(ODATION)?|STORAGE|WARE(HOUSE|HSE)|GARAGES|GYM(NASIUM)?|SHOPPING\s(CENTER|CTR)?)\M'
                    AND dvr_commercial.improvements_value > 0
                    THEN 1
                    ELSE 4
                END
                WHEN (
                    dvr_commercial.actual_property_use IS NULL 
                    AND dvr_commercial.category ~ '^C'
                )
                THEN 9
                WHEN
                    dvr_commercial.actual_property_use = '89' -- Vacant
                    OR dvr_commercial.improvements_description IS NULL
                    OR dvr_commercial.improvements_value = 0
                THEN 11

                WHEN dvr_commercial.category ~ '^C'
                THEN 10
                ELSE NULL
            END,
            ARRAY[]::TEXT[],
            ARRAY[]::TEXT[],
            ARRAY[dvr_commercial.source_data]::TEXT[],
            dvr_commercial.source_date,
            dvr_commercial.source_scale
        )::nzlum_type
    END AS nzlum_type
    FROM (
        SELECT * FROM linz_dvr_
        WHERE (
            actual_property_use ~ '^8'
            OR actual_property_use = '08'
            OR actual_property_use = '78' -- Industrial, depots and yards
            OR category ~ '^C' -- Commercial
        )
        AND (
            improvements_description IS NULL
            OR improvements_description !~ '\m(SKIFIELD|VACANT\sLAND)\M'
        )
    ) dvr_commercial
    FULL OUTER JOIN (
        SELECT *,
        'LINZ' AS source_data,
        daterange(
            last_modified::DATE,
            last_modified::DATE,
            '[]'
        ) AS source_date, -- source_date
        CASE
            WHEN urban_rural_2025_.IUR2025_V1_00 IN (
                '11', -- major urban
                '12', -- large urban
                '13', -- medium urban
                '14' -- small urban
            ) -- 0.1 - 1 m in urban areas
            THEN '(0,1]'::int4range
            WHEN urban_rural_2025_.IUR2025_V1_00 IN (
                '21', -- rural settlement
                '22' -- rural other
            ) -- 1 - 100 m in rural areas
            THEN '[1,100]'::int4range
            ELSE 'empty'::int4range
        END AS source_scale-- source_scale
        FROM nz_facilities
        JOIN nz_facilities_h3 USING (ogc_fid)
        LEFT JOIN (
            SELECT
                urban_rural_2025_h3.h3_index,
                urban_rural_2025.IUR2025_V1_00
            FROM urban_rural_2025_h3
            JOIN urban_rural_2025 USING (ogc_fid)
            WHERE :parent::h3index = h3_partition
        ) AS urban_rural_2025_ USING (h3_index)
        WHERE nz_facilities.use_type NOT IN (
            'NGO Hospital' -- Not public
        )
    ) private_hospitals USING (h3_index)
);