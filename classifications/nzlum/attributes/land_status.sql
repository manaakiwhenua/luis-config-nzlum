-- See p354, Bellingham PJ, Overton JMcC, Thomson FJ, MacLeod CJ, Holdaway RJ, Wiser SK, Brown M, Gormley AM, Collins D, Latham DM, Bishop C, Rutledge DT, Innes JG, Warburton B 2016 Standardised terrestrial biodiversity indicators for use by regional councils. Landcare Research Contract Report LC2109 prepared for Regional Councils' Biodiversity Monitoring Working Group, Auckland Council, Auckland, New Zealand
CREATE TEMPORARY VIEW land_status AS (
    -- TODO make this a separate attribute and index?
    WITH cleaned_crosl AS (
        SELECT
            ogc_fid,
            gov_type,
            lowercase_conjunctions(managed_by) AS managed_by
        FROM crosl
    ),
    crosl_land_status AS (
        SELECT 
            h3_index,
            'CRoSL' AS _source,
            CASE
                WHEN gov_type = 'Local'

                    THEN CASE

                        WHEN managed_by ~ '\mDistrict Council\M'
                            THEN ARRAY[
                                'Territorial Local Authorities',
                                'District Council',
                                managed_by
                            ]

                        WHEN managed_by ~ '\mCity Council\M'
                            THEN ARRAY[
                                'Territorial Local Authorities',
                                'City Council',
                                managed_by
                            ]

                        WHEN managed_by ~ '\mRegional Council\M'
                            THEN ARRAY[
                                'Territorial Local Authorities',
                                'Regional Council',
                                managed_by
                            ]

                        WHEN managed_by ~ '\mCouncil\M'
                            THEN ARRAY[
                                'Territorial Local Authorities',
                                'Council',
                                managed_by
                            ]

                    END

                WHEN gov_type = 'Central' THEN CASE

                    WHEN managed_by IN (
                        'Accident Compensation Corporation',
                        'Housing New Zealand',
                        'Callaghan Innovation',
                        'Fire and Emergency New Zealand',
                        'Maritime New Zealand',
                        'New Zealand Transport Agency',
                        'Health New Zealand'
                    ) OR managed_by ~ '\mDistrict Health Board\M' -- NB defunct
                        THEN ARRAY[
                            'Crown entities',
                            'Statutory entities',
                            'Crown agents',
                            CASE WHEN managed_by ~ '\mDistrict Health Board\M' THEN 'Health New Zealand' ELSE managed_by END
                        ]
                    
                    WHEN managed_by IN (
                        'Heritage New Zealand Pouhere Taonga',
                        'Museum of New Zealand Te Papa Tongarewa Board'
                    )
                        THEN ARRAY[
                            'Crown entities',
                            'Statutory entities',
                            'Autonomous Crown entities',
                            managed_by
                        ]

                    WHEN managed_by IN (
                        'Airways Corporation of New Zealand Limited',
                        -- 'Solid Energy New Zealand Limited', -- Defunct since 16 March 2018, should not be included
                        'Animal Control Products Limited',
                        'Kordia Group Limited',
                        'Landcorp Farming Limited',
                        'Meteorological Service of New Zealand Limited',
                        'New Zealand Post Limited',
                        'New Zealand Railways Corporation',
                        'Transpower New Zealand Limited'
                    )
                        THEN ARRAY[
                            'State-owned enterprises',
                            managed_by
                        ]

                    WHEN managed_by IN (
                        'Agresearch Limited',
                        'Landcare Research New Zealand Limited',
                        'National Institute of Water and Atmospheric Research Limited',
                        'Institute of Environmental Science and Research Limited',
                        'Institute of Geological and Nuclear Sciences Limited',
                        'The New Zealand Institute For Plant and Food Research Limited'
                    )
                        THEN ARRAY[
                            'Crown entities',
                            'Crown entity companies',
                            'Crown Research Institutes',
                            CASE WHEN managed_by = 'Agresearch Limited' THEN 'AgResearch Limited' ELSE managed_by END
                        ]
                    
                    WHEN managed_by IN (
                        'Radio New Zealand Limited',
                        'Television New Zealand Limited'
                    )
                        THEN ARRAY[
                            'Crown entities',
                            'Crown entity companies',
                            managed_by
                        ]
                    
                    WHEN managed_by IN (
                        'Reserve Bank of New Zealand'
                    )
                        THEN ARRAY [
                            'Sui generis organisations',
                            managed_by
                        ]

                    WHEN managed_by IN (
                        'Department of Conservation',
                        'Land Information New Zealand',
                        'Ministry of Education',
                        'Department of Internal Affairs'
                    )
                        THEN ARRAY[
                            'Public Service',
                            'Departments',
                            managed_by
                        ]

                    WHEN managed_by IN (
                        'New Zealand Defence Force',
                        'New Zealand Police'
                    )
                    THEN ARRAY[
                        'Non-Public Service departments',
                        'Executive branch',
                        managed_by
                    ]   

                    WHEN managed_by IN (
                        'Fish and Game'
                    )
                        THEN ARRAY[
                            'Public Finance Act 1989 Schedule 4 Organisations',
                            'Fish and Game Councils'
                        ]

                    WHEN managed_by IN (
                        'Ports of Auckland Limited' -- Neither a CCO or CCTO, as it is a port?
                    )
                        THEN ARRAY[
                            'Council organisations', -- https://oag.parliament.nz/2015/cco-governance/part1.htm
                            managed_by
                        ]   

                    WHEN managed_by IN (
                        'Palmerston North Airport Limited'
                    )
                        THEN ARRAY[
                            'Council organisations', -- https://oag.parliament.nz/2015/cco-governance/part1.htm
                            'Council-controlled trading organisations', -- Profiting-making enterprises
                            managed_by
                        ]
                    
                    WHEN managed_by IN (
                        'The Tauranga Art Gallery Trust',
                        'Bay Venues Limited',
                        'Watercare Services Limited'
                    )
                    THEN ARRAY[
                            'Council organisations', -- https://oag.parliament.nz/2015/cco-governance/part1.htm
                            'Council-controlled organisations', -- Non-profit, and excludes ports, energy companies, electricity lines businesses and their parent trusts, and several other named entities
                            managed_by
                        ]

                    WHEN managed_by IN (
                        'Genesis Energy Limited',
                        'Meridian Energy Limited',
                        'Mighty River Power Limited - Grantee'
                    )
                        THEN ARRAY[
                            'Mixed ownership model companies',
                            CASE WHEN managed_by = 'Mighty River Power Limited - Grantee' THEN 'Mercury NZ Limited' ELSE managed_by END
                        ]


                    WHEN managed_by ~ '\m(University|W(a|ā)nanga)\M'
                        THEN ARRAY[
                        'Crown entities',
                        'Tertiary education institutions',
                        managed_by
                    ]

                    WHEN managed_by IN (
                        'City Rail Link Limited'
                    )
                        THEN ARRAY[
                            'Public Finance Act 1989 Schedule 4A Companies',
                            managed_by
                        ]

                    -- TODO 
                    -- "The Wellington Regional Stadium Trust Incorporated" (charitable trust)
                END
            END AS land_status
        FROM cleaned_crosl
        JOIN crosl_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
    ),
    pan_nz_draft_land_status AS (
        SELECT
            DISTINCT ON (h3_index)
            h3_index,
            'PAN-NZ' AS _source,
            CASE
                WHEN legislation_act = 'RESERVES_ACT' AND legislation_section IN (
                    'S17_RECREATION_RESERVE', '17_RECREATION_RESERVE',
                    'S19_AMBIGUOUS',
                    'S19_1_A_SCENIC_RESERVE',
                    'S19_1_B_SCENIC_RESERVE',
                    'S23_LOCAL_PURPOSE_RESERVE'
                ) OR legislation_act = 'LOCAL_GOVT_MANAGED_AREA'
                    THEN ARRAY[
                        'Territorial Local Authorities'
                    ]

                WHEN legislation_act = 'WELLINGTON_TOWN_BELT_ACT_2016'
                    THEN ARRAY[
                        'Territorial Local Authorities',
                        'City Council',
                        'Wellington City Council'
                    ]

                WHEN legislation_act = 'TE_TURE_WHENUA_MAORI_ACT' AND legislation_section = 'MAORI_RESERVATION'
                    THEN ARRAY[
                        'Private',
                        'Māori Reservation'
                    ]
                
                WHEN legislation_act = 'LOCAL_GOVERNMENT_ACT' AND legislation_section IN ('S139_REGIONAL_PARKS', 'S139_REGIONAL_PARK')
                    THEN ARRAY[
                        'Territorial Local Authorities',
                        'Regional Council'
                    ]

                WHEN (
                    legislation_act = 'CONSERVATION_AREA'
                    AND legislation_section = 'S25_STEWARDSHIP_AREA'
                ) OR  legislation_act IN (
                    'NATIONAL_PARK_ACT', 'NATIONAL_PARKS_ACT',
                    'CONSERVATION_ACT',
                    'MARINE_MAMMALS_PROTECTION_ACT',
                    'MARINE_RESERVES_ACT',
                    'WILDLIFE_ACT'
                )
                    THEN ARRAY[
                        'Public Service',
                        'Departments',
                        'Department of Conservation'
                    ]
            END AS land_status
        FROM pan_nz_draft
        JOIN pan_nz_draft_h3 USING (ogc_fid)
        WHERE :parent::h3index = h3_partition
        AND NOT EXISTS (
            -- Only if not present in CROSL
            SELECT 1 FROM crosl_land_status WHERE crosl_land_status.h3_index = pan_nz_draft_h3.h3_index
        )
        ORDER BY
            h3_index,
            source_date DESC NULLS LAST, -- Prefer more recent
            CASE
                WHEN iucn_category = 'Ia' THEN 1
                WHEN iucn_category = 'Ib' THEN 2
                WHEN iucn_category = 'II' THEN 3
                WHEN iucn_category = 'III' THEN 4
                WHEN iucn_category = 'IV' THEN 5
                WHEN iucn_category = 'V' THEN 6
                WHEN iucn_category = 'Not IUCN' THEN 7
                WHEN iucn_category = 'Not Mapped' THEN 9
                ELSE NULL
            END ASC NULLS LAST, -- Prefer greater protection status
            source_id -- Tie-break
    )
    SELECT DISTINCT ON (h3_index) *
    FROM (
        SELECT * FROM crosl_land_status
        WHERE land_status IS NOT NULL
        UNION ALL
        SELECT * FROM pan_nz_draft_land_status
        WHERE land_status IS NOT NULL
    ) combined
    
    ORDER BY h3_index, CASE
        WHEN _source = 'CRoSL' THEN 0
        WHEN _source = 'PAN-NZ' THEN 1
    END ASC NULLS LAST -- Prefer CRoSL if there's a conflict or duplicate
);

-- TODO https://data.linz.govt.nz/table/105627-nz-properties-ownership/
-- this is already in linz_dvr as 'ownership code'
-- BUt there are also values 0, 1, 2 etc. which mean ???
-- 3 Public: Core Crown, e.g. ministry/department
-- 4 Public: Local authority
-- 5 Public: Non-Core Crown, e.g. state-owned enterprise/hospital/education/administering body of Crown Land
-- 6 Private: Māori - individual
-- 7 Private: Māori - tribal/incorporations/many owners

-- TODO Maori land court: https://api.service.maorilandcourt.govt.nz/geoserver/wfs/mlgis