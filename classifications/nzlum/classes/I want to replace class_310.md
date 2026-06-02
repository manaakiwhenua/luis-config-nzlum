I want to replace class_310.sql with the ertiary classes, 311, 312, 313, 314.

3.1.1 High-density residential
Areas characterised by a high concentration of housing units per unit of land area, typically in multi-storey buildings or high-rise developments, often found in urban centres supporting high population density.

3.1.2 Medium-density residential
Areas featuring a moderate concentration of housing units per unit of land area, often in suburban or semi-urban settings.
Example: Townhouses, terraced housing, low-rise apartments.

3.1.3 Low-density residential
Residential properties within urban boundaries that fall within large-lot or low-density residential zones and are often single-family, one- to two- storey houses with yards and landscaping and lower population density.

3.1.4 Rural residentual
Residential properties with low-intensity (non-commercial) land management practices on land in rural or peri-urban areas.
Scope note: Typically featuring larger parcel sizes amidst agricultural or natural surroundings. Concordant with the‘Rural lifestyle zone’ from the Zone Framework Standard.
Usage note: As a general guideline, properties with a dwelling, and between 0.4 ha and 2 ha in size, should be considered rural residential. Larger properties should be considered under class 2, especially if the zone context is non-residential, and even if they are declared residential or lifestyle in the District Valuation Roll.
Usage note: Properties in this size interval and without a dwelling could be in transition to residential.

---

Implementation details:

- "Dwellings" can be determined with limited confidence by looking at LINZ DVR improvements description, specifically by finding the string "DWG".
- "TOWN HOUSE" OR "THSE" is an improvments description that clearly identifies 3.1.2.
- "APARTMENT" indicates 3.1.1
- Similarly for "FLAT", suggests 3.1.2 mainly, but possibly 3.1.3. Defintely NOT 3.1.1 or 3.1.4.
- "\d UNIT" (e.g. "4 UNIT") suggests 3.1.1 or 3.1.2
- "\d FLAT" is also possible, suggestes 3.1.2
- "\d DWG" may indicate 3.1.1 (if \d is large), or 3.1.2 (if area is small for \d) else 3.1.3, but still possibly 3.1.4
- "REST" indicates a resthome, suggests 3.1.2?
- "DWG .* POOL" suggests 3.1.3 or 3.1.4
- "SLEEP OUT" or "STUDIO" sugggests 3.1.3 or 3.1.4
- Size can be determined using LINZ DVR land_area (hectares)
- DVR capital_value = improvements_value + land_value, and may give an indicator?
- "DBLEGGE" is double garage, and indicates 3.1.3 or 3.1.4
- "BACH" indicates a holiday home, probably 3.1.3, maybe 3.1.4
- Taking evidence combined may help, e.g. "DWG GGE OI POOL" means a dwelling (DWG), with a garage (GGE), a pool, and other improvements (OI).
- "COTTAGE" supports a 3.1.3/3.1.4
- "BRD HOUSE" is a boarding house, 3.1.1 or 3.1.2
- "RES ACCOM" is residential accomodation, 3.1.1 or 3.1.2
- "PT BLDG" is part building, and suggests 3.1.1 or 3.1.2
- "PT FLR" is part floor, and suggests 3.1.1 or 3.1.2
- "TCE HSE" is a terrace house, suggests 3.1.2?
- You can read more sample common improvement code abbreviations here: https://www.aucklandcouncil.govt.nz/en/property-rates-valuations/our-valuation-of-your-property/description-of-improvement-codes.html

- NB this is important: it seems that separate inhabited dwellings are recorded as different units. So where mutiple units exist, we're getting a good indication of 3.1.1 and 3.1.2. The primary purpose of the DVR is to form the basis of municipal taxes, referred to as council rates. These are applied to all the properties, or ‘‘rating units’’, within the council’s jurisdiction. A rating unit generally refers to a portion of land or a property with an individual ‘‘record of title’’, which is a legal record held by LINZ that describes the legal owner(s), boundaries, rights, and restrictions applied to a property.5 A record of title can encompass multiple properties, for example, one legal property that contains multiple, separate dwellings. These are generally entered as one rating unit in the DVR, and assigned multiple ‘‘units of use’’ corresponding to the total number of separately used or inhabited parts (SUIPs) of the property.
- The RVA allows for multiple “units of use” to be applied to an individual rating unit. This accords with local councils generally needing to provide services on a per unit of use basis, rather than per legal property or per entry in the DVR. - uckland Council classifies units of use based on the “separately used or inhabited parts” (SUIPs) of a property. An SUIP is defined as “any part of a rating unit that is separately used or inhabited by the ratepayer, or by any other person having a right to use or inhabit that part by virtue of a tenancy, lease, licence or any other agreement”.8 Under this definition, parts of a rating unit will be treated as “separately used” if they have different use categories. For example, a shop with accommodation above will be treated as two rating units. Similarly, multiple instances of the same use category will also be classified as separately used, for example if a property contains multiple commercial outlets, such as a food court or shopping centre. In the same vein, a residential property with a separate dwelling, such as a self-contained ‘‘granny flat’’, will be classified as having two SUIPs.
- If the separate parts of a rateable unit are contiguous,10 and used by the same owner(s) as a single unit, then they are classified as one SUIP. For example, a residential property with a self-contained granny flat will count as one SUIP if the flat is internally accessible from the main residence, and both parts are used together as a single family home.
- Commercial accommodation, such as motels, hotels, and some rest homes, are treated as having one SUIP, regardless of the number of rooms.
- Each record within the DVR is assigned an ‘‘actual property use’’. This field allows us to distinguish residential units from units used for other purposes, such as commercial. The implementation rules produced by the Valuer General to direct councils in producing the DVR contain prescriptive categories to describe the actual property use of a rating unit. This is defined as ‘‘the activity, or group of interdependent activities having a common purpose, performed on land or building floor space at the date of inspection’’. This is captured through a two-character numerical code referring to the primary and secondary level. The primary code refers to the broad classification, such as rural, industrial, commercial or residential. The secondary codes are subcategories within the broad classification. For example, within the primary level code 9, which denotes ‘‘Residential’’, there are secondary codes referring to if the property is a single unit or part of a multi-unit complex.
- Specific codes exist to capture situations of ‘‘multi-use’’, where the multiple uses for a rating unit do not fall within the same use category. When multi-use occurs within a broad use category, such as ‘‘commercial’’ or ‘‘residential’’, the secondary code will indicate multi-use. For example
a commercial property with two separate commercial uses, such as retail and offices, would be classified as code 80. This is made up of primary code 8 for ‘‘Commercial’’ and secondary code 0 for ‘‘Multi-use within commercial’’
- Primary code 0 refers to the situation where multiple uses occur at the broad classification level. For example, commercial shops on the ground floor of a building with residential accommodation above. In these cases, the secondary code refers to ‘‘major-use’’, which is the broad use category which contributes the greatest proportion of assessed rental.15 If assessed rents are equal, the use with the greatest floor area is determined to be the major use. For example, in the case of the shops with accommodation, the code would be 08 for commercial or 09 for residential, depending on which category – commercial or residential – represented the major use.
- Although the categories are prescriptive, the ratings valuation rules provide no specific definitions on how to classify a property use into each category. This lack of guidance is arguably less relevant for the primary level categories, such as commercial or residential,which have self-evident definitions. But it is relevant for the secondary classification code. In practice, classification is generally left to the ratings valuers, who have typically taken a ‘‘common sense’’ approach to determining the appropriate use category.
- Some codes:
    - 02 (multi-use at the primary level; lifestyle)
    - 09 (mutli-use at the primary level; residential)
    - 20 (lifestyle; multi-use within lifestyle)
    - 21 (lifestyle; single-unit)
    - 22 (lifestyle; multi-unit)
    - 29 (lifestyle; vacant)
    - 90 (residential; multi-use within residential)
    - 91 (residential; single unit excluding bach), e.g. a stand-alone dwelling on a single lot
    - 92 (residential; multi-unit), e.g. cross-leased properties, units, flats, town houses, multuiple houses
    - 93 (residential; public communal unlicensed), e.g. motels, holiday parks, campgrounds, guest houses