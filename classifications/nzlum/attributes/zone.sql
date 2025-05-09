CREATE TEMPORARY VIEW land_zone AS (
    SELECT
        h3_index,
        CASE -- TODO these could be refined, see Table 19 (https://environment.govt.nz/assets/publications/national-planning-standards-november-2019-updated-2022.pdf) and Section C.2 (https://www.linz.govt.nz/sites/default/files/30300-Rating%2520Valuations%2520Rules%25202008-%2520version%2520date%25201%2520October%25202010%2520-%2520LINZS30300_0.pdf)
            WHEN "zone" = '0X' THEN 'Land in more than one zone or designation' -- Land in more than one zone or designation
            WHEN "zone" = 'OX' THEN 'Land in more than one zone or designation' -- Assume this is a rare typo for the same
            WHEN "zone" LIKE '0%' THEN 'Designated or zoned reserve land' -- Designated or zoned reserve land
            WHEN "zone" LIKE 'O%' THEN 'Designated or zoned reserve land' -- Assume this is a rare typo for the same
            WHEN "zone" LIKE '1%' THEN 'Rural'
            WHEN "zone" LIKE '2%' THEN 'Lifestyle'
            WHEN "zone" LIKE '3%' THEN 'Other specific zone'
            WHEN "zone" LIKE '4%' THEN 'Community uses'
            WHEN "zone" LIKE '5%' THEN 'Recreational'
            WHEN "zone" LIKE '6%' THEN 'Other broad zone'
            WHEN "zone" LIKE '7%' THEN 'Industrial'
            WHEN "zone" LIKE '8%' THEN 'Commercial'
            WHEN "zone" LIKE '9%' THEN 'Residential'
            ELSE null
        END AS "zone", -- May be better to just take linz_dvr_.zone values? Except that there are invalid codes
        land_estate
    FROM linz_dvr_
);