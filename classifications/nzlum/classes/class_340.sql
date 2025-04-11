CREATE TEMPORARY VIEW class_340 AS ( -- Manufacturing and industrial
    SELECT h3_index,
    3 AS lu_code_primary,
    4 AS lu_code_secondary,
    0 AS lu_code_tertiary,
    ROW(
        ARRAY[]::TEXT[], -- lu_code_ancillary
        CASE
            WHEN linz_dvr_industrial.h3_index IS NOT NULL
            THEN
                LEAST(GREATEST(CASE
                    WHEN
                        actual_property_use <> '07'
                        AND actual_property_use NOT LIKE '%0'
                        AND improvements_value > 0
                    THEN 2
                    WHEN actual_property_use = '70' -- Multi-use within industrial
                    THEN
                        CASE
                            WHEN improvements_value > 0
                            THEN 2
                            ELSE 5
                        END
                    WHEN
                        (
                            actual_property_use LIKE '79' -- Vacant
                            OR actual_property_use = '07'
                        )
                        AND (
                            improvements_description IS NULL
                            OR improvements_value = 0
                        )
                    THEN 11
                    WHEN linz_dvr_industrial.h3_index IS NOT NULL
                    THEN 8
                    ELSE NULL
                END
                +
                -- Potential supplementary confidence from HAIL
                CASE
                    WHEN hail_manufacturing_and_industrial.h3_index IS NOT NULL
                    THEN -1
                    ELSE 0
                END, 1), 12)
            WHEN  hail_manufacturing_and_industrial.h3_index IS NOT NULL
            THEN 12 -- Residual possibility
            ELSE NULL
        END, -- Confidence
        ARRAY[]::TEXT[],
        ARRAY[]::TEXT[],
        ARRAY[hail_manufacturing_and_industrial.source_data, linz_dvr_industrial.source_data]::TEXT[],
        range_merge(datemultirange(
            VARIADIC ARRAY_REMOVE(ARRAY[
                hail_manufacturing_and_industrial.source_date,
                linz_dvr_industrial.source_date
            ], NULL)
        ))::daterange, -- source_date
        range_merge(int4multirange(
            VARIADIC ARRAY_REMOVE(ARRAY[
                hail_manufacturing_and_industrial.source_scale,
                linz_dvr_industrial.source_scale
            ], NULL)
        ))::int4range -- source_scale
    )::nzlum_type AS nzlum_type
    FROM (
        SELECT * FROM linz_dvr_
        WHERE (
            actual_property_use LIKE '7%'
            OR actual_property_use = '07'
        ) AND NOT actual_property_use = '78' -- Depots and yards; considered commercial not industrial
    ) AS linz_dvr_industrial
    FULL OUTER JOIN (
        -- As the manufacture, use, storage and disposal of contaminants is confused (and often historical), HAIL itself offers strong supplementary evidence but weak primary evidence
        SELECT *
        FROM hail
        WHERE hail_category_ids @> ARRAY[
            'A1', -- Agrichemicals
            'A2', -- Chemical manufacture
            'A3', -- Commercial analytical laboratory sites
            'A4', -- Corrosives (forumlation or bulk storage)
            'A5', -- Dry cleaning
            'A6', -- Fertiliser manufacture
            'A7', -- Gasworks
            'A9', -- Paint manufacturing (excl. retail paint stores)
            'A10', -- Persistent pesticide bulk storage or use
            'A11', -- Pest control
            'A12', -- Pesticide manufacture
            'A13', -- Petroleum/petrochemical industry or bulk storage
            'A14', -- Pharmaceutical manufacture
            'A15', -- Printing
            'A16', -- Skin or wool processing, tannery, fellmongery
            'A17', -- Storage tanks or drums for fuel, chemicals, liquid waste
            'A18', -- Wood treatment or preservation incl bulk stoage of treated timber outside
            'B1', -- Batteries
            'B2', -- Electric transfotmers including manufacturing
            'B3', -- Electronics manufacturing, reconditioning, recycling
            'C1', -- Explosive or ordinance production, etc.
            'D1', -- Metal - abrasive blasting
            'D2', -- Foundary
            'D3', -- Metal treatment or coating
            'D4', -- Metalliferous ore processing
            'D5', -- Engineering workshops with metal fabrication
            'E1', -- Asbestos manufacture or disposal
            'E2', -- Asphalt or bitumen
            'E3', -- Cement or lime
            'E4', -- Concrete
            'E5', -- Coal or coke
            'E6', -- Hydrocrabon exploration or production including well sites
            'E7', -- Mining excl. gravel extraction
            'F2', -- Brake lining
            'F3', -- Engine reconditioning
            'F4', -- Motor vehicle workshops
            'F7', -- Service stations including commercial refuelling facilities
            'G2', -- Drum or tank reconditioning or recyling
            'G4' -- Scrap yards including automotive dismantling, wrecking, or scrap metal
        ]
    ) AS hail_manufacturing_and_industrial USING (h3_index)
);