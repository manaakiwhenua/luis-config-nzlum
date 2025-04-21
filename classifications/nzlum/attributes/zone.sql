CREATE TEMPORARY VIEW land_zone AS (
    SELECT
        h3_index,
        CASE -- TODO these could be refined, see Table 19 (https://environment.govt.nz/assets/publications/national-planning-standards-november-2019-updated-2022.pdf) and Section C.2 (https://www.linz.govt.nz/sites/default/files/30300-Rating%2520Valuations%2520Rules%25202008-%2520version%2520date%25201%2520October%25202010%2520-%2520LINZS30300_0.pdf)
            WHEN "zone" = '0X' THEN null -- Land in more than one zone or designation
            WHEN "zone" LIKE '0%' THEN null -- Designated or zoned reserve land
            WHEN "zone" LIKE '1%' THEN 'General rural zone' -- Rural
            WHEN "zone" LIKE '2%' THEN 'Rural lifestyle zone' -- Lifestyle
            WHEN "zone" LIKE '3%' THEN 'Special purpose zone' -- Other specific zone - defined by territorial authority
            WHEN "zone" LIKE '4%' THEN null -- Community uses
            WHEN "zone" LIKE '5%' THEN 'Sport and active recreation zone' -- Recreational
            WHEN "zone" LIKE '6%' THEN null -- Other broad zone - defined by territorial authority
            WHEN "zone" LIKE '7%' THEN 'General industrial zone' -- Industrial
            WHEN "zone" LIKE '8%' THEN 'Commercial zone' -- Commercial
            WHEN "zone" LIKE '9%' THEN 'General residential zone' -- Residential
            ELSE null
        END AS "zone", -- May be better to just take linz_zone.zone values?, 
        land_estate
    FROM linz_dvr_
);