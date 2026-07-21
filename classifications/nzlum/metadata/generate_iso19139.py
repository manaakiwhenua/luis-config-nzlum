#!/usr/bin/env python3
"""Generate an ISO 19139 (gmd) XML metadata record from metadata.iso19115.json.

metadata.iso19115.json is the single source of truth for this dataset's
discovery/catalogue metadata. This script derives metadata.iso19139.xml from
it; the XML is a build artefact and should not be hand-edited - re-run this
script after changing the JSON.

Usage:
    python3 generate_iso19139.py [metadata.iso19115.json] [metadata.iso19139.xml]
"""
import json
import sys
from pathlib import Path
from xml.etree import ElementTree as ET

NS = {
    "gmd": "http://www.isotc211.org/2005/gmd",
    "gco": "http://www.isotc211.org/2005/gco",
    "gml": "http://www.opengis.net/gml/3.2",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}
CODELIST_BASE = "https://standards.iso.org/iso/19139/resources/gmxCodelists.xml"

for prefix, uri in NS.items():
    ET.register_namespace(prefix, uri)


def q(prefix, tag):
    return f"{{{NS[prefix]}}}{tag}"


def char_string(parent, tag, text):
    el = ET.SubElement(parent, q("gmd", tag))
    cs = ET.SubElement(el, q("gco", "CharacterString"))
    cs.text = text
    return el


def codelist(parent, tag, codelist_name, value, text=None):
    el = ET.SubElement(parent, q("gmd", tag))
    code_el = ET.SubElement(
        el,
        q("gmd", codelist_name),
        {
            "codeList": f"{CODELIST_BASE}#{codelist_name}",
            "codeListValue": value,
        },
    )
    code_el.text = text if text is not None else value
    return el


def responsible_party(parent, tag, party):
    el = ET.SubElement(parent, q("gmd", tag))
    crp = ET.SubElement(el, q("gmd", "CI_ResponsibleParty"))
    if party.get("individualName"):
        char_string(crp, "individualName", party["individualName"])
    if party.get("organisationName"):
        char_string(crp, "organisationName", party["organisationName"])
    if party.get("positionName"):
        char_string(crp, "positionName", party["positionName"])
    if party.get("electronicMailAddress"):
        contact_info = ET.SubElement(crp, q("gmd", "contactInfo"))
        ci = ET.SubElement(contact_info, q("gmd", "CI_Contact"))
        address = ET.SubElement(ci, q("gmd", "address"))
        ci_address = ET.SubElement(address, q("gmd", "CI_Address"))
        char_string(ci_address, "electronicMailAddress", party["electronicMailAddress"])
    codelist(crp, "role", "CI_RoleCode", party.get("role", "pointOfContact"))
    return el


def build(md_json):
    root = ET.Element(q("gmd", "MD_Metadata"))

    char_string(root, "fileIdentifier", md_json["fileIdentifier"])
    codelist(root, "language", "LanguageCode", md_json.get("language", "eng"))
    codelist(root, "characterSet", "MD_CharacterSetCode", md_json.get("characterSet", "utf8"))
    codelist(root, "hierarchyLevel", "MD_ScopeCode", md_json.get("hierarchyLevel", "dataset"))

    for contact in md_json.get("contact", []):
        responsible_party(root, "contact", contact)

    date_el = ET.SubElement(root, q("gmd", "dateStamp"))
    ET.SubElement(date_el, q("gco", "Date")).text = md_json["dateStamp"]

    char_string(root, "metadataStandardName", md_json.get("metadataStandardName", "ISO 19115"))
    char_string(root, "metadataStandardVersion", md_json.get("metadataStandardVersion", ""))

    for rsi in md_json.get("referenceSystemInfo", []):
        rsi_el = ET.SubElement(root, q("gmd", "referenceSystemInfo"))
        md_rs = ET.SubElement(rsi_el, q("gmd", "MD_ReferenceSystem"))
        rs_id = ET.SubElement(md_rs, q("gmd", "referenceSystemIdentifier"))
        rs_identifier = ET.SubElement(rs_id, q("gmd", "RS_Identifier"))
        char_string(rs_identifier, "code", rsi["code"])
        char_string(rs_identifier, "codeSpace", rsi.get("codeSpace", ""))
        if rsi.get("note"):
            rsi_el.append(ET.Comment(f" {rsi['name']}: {rsi['note']} "))

    spatial_rep = md_json.get("spatialRepresentationInfo")
    if spatial_rep:
        sri_el = ET.SubElement(root, q("gmd", "spatialRepresentationInfo"))
        vsr = ET.SubElement(sri_el, q("gmd", "MD_VectorSpatialRepresentation"))
        codelist(vsr, "topologyLevel", "MD_TopologyLevelCode", spatial_rep.get("topologyLevel", "geometryOnly"))
        geom_obj = ET.SubElement(vsr, q("gmd", "geometricObjects"))
        mgo = ET.SubElement(geom_obj, q("gmd", "MD_GeometricObjects"))
        codelist(mgo, "geometricObjectType", "MD_GeometricObjectTypeCode", "surface")
        dggs = spatial_rep.get("dggs")
        if dggs:
            sri_el.append(ET.Comment(
                f" DGGS: {dggs['system']}, resolution {dggs['resolution']}"
                f" (avg. edge {dggs['averageEdgeLengthMetres']} m,"
                f" avg. cell area {dggs['averageCellAreaKm2']} km2)."
                f" {dggs.get('note', '')} "
            ))

    ident = md_json["identificationInfo"]
    ident_el = ET.SubElement(root, q("gmd", "identificationInfo"))
    data_ident = ET.SubElement(ident_el, q("gmd", "MD_DataIdentification"))

    citation_el = ET.SubElement(data_ident, q("gmd", "citation"))
    ci_citation = ET.SubElement(citation_el, q("gmd", "CI_Citation"))
    cit = ident["citation"]
    char_string(ci_citation, "title", cit["title"])
    if cit.get("alternateTitle"):
        char_string(ci_citation, "alternateTitle", cit["alternateTitle"])
    dates = cit["date"] if isinstance(cit["date"], list) else [cit["date"]]
    for date in dates:
        cd = ET.SubElement(ci_citation, q("gmd", "date"))
        ci_date = ET.SubElement(cd, q("gmd", "CI_Date"))
        date_val = ET.SubElement(ci_date, q("gmd", "date"))
        ET.SubElement(date_val, q("gco", "Date")).text = date["value"]
        codelist(ci_date, "dateType", "CI_DateTypeCode", date["type"])
        if date.get("note"):
            cd.append(ET.Comment(f" {date['note']} "))
    char_string(ci_citation, "edition", cit["edition"])
    ed_date = ET.SubElement(ci_citation, q("gmd", "editionDate"))
    ET.SubElement(ed_date, q("gco", "Date")).text = cit["editionDate"]
    for party in cit.get("citedResponsibleParty", []):
        responsible_party(ci_citation, "citedResponsibleParty", party)
    identifiers = cit.get("identifier", [])
    identifiers = identifiers if isinstance(identifiers, list) else [identifiers]
    for identifier in identifiers:
        id_el = ET.SubElement(ci_citation, q("gmd", "identifier"))
        md_identifier = ET.SubElement(id_el, q("gmd", "MD_Identifier"))
        char_string(md_identifier, "code", identifier["code"])
        char_string(md_identifier, "codeSpace", identifier.get("codeSpace", ""))
        if identifier.get("description"):
            id_el.append(ET.Comment(f" {identifier['description']} "))
    if cit.get("otherCitationDetails"):
        char_string(ci_citation, "otherCitationDetails", cit["otherCitationDetails"])

    char_string(data_ident, "abstract", ident["abstract"])
    if ident.get("purpose"):
        char_string(data_ident, "purpose", ident["purpose"])
    for party in ident.get("pointOfContact", []):
        responsible_party(data_ident, "pointOfContact", party)
    codelist(data_ident, "status", "MD_ProgressCode", ident.get("status", "onGoing"))

    maint = ident.get("resourceMaintenance")
    if maint:
        maint_el = ET.SubElement(data_ident, q("gmd", "resourceMaintenance"))
        mmi = ET.SubElement(maint_el, q("gmd", "MD_MaintenanceInformation"))
        codelist(mmi, "maintenanceAndUpdateFrequency", "MD_MaintenanceFrequencyCode",
                 maint.get("maintenanceAndUpdateFrequency", "asNeeded"))
        if maint.get("note"):
            char_string(mmi, "maintenanceNote", maint["note"])

    for kw_group in ident.get("descriptiveKeywords", []):
        dk_el = ET.SubElement(data_ident, q("gmd", "descriptiveKeywords"))
        md_kw = ET.SubElement(dk_el, q("gmd", "MD_Keywords"))
        for kw in kw_group["keywords"]:
            char_string(md_kw, "keyword", kw)
        codelist(md_kw, "type", "MD_KeywordTypeCode", kw_group["type"])

    res = ident.get("spatialResolution")
    if res:
        res_el = ET.SubElement(data_ident, q("gmd", "spatialResolution"))
        md_res = ET.SubElement(res_el, q("gmd", "MD_Resolution"))
        dist_el = ET.SubElement(md_res, q("gmd", "distance"))
        ET.SubElement(dist_el, q("gco", "Distance"), {"uom": res["distance"]["uom"]}).text = str(res["distance"]["value"])
        if res.get("note"):
            res_el.append(ET.Comment(f" {res['note']} "))

    for topic in ident.get("topicCategory", []):
        tc_el = ET.SubElement(data_ident, q("gmd", "topicCategory"))
        ET.SubElement(tc_el, q("gmd", "MD_TopicCategoryCode")).text = topic

    extent = ident.get("extent")
    if extent:
        extent_el = ET.SubElement(data_ident, q("gmd", "extent"))
        ex_extent = ET.SubElement(extent_el, q("gmd", "EX_Extent"))
        geo = extent.get("geographicElement")
        if geo:
            geo_el = ET.SubElement(ex_extent, q("gmd", "geographicElement"))
            bbox = ET.SubElement(geo_el, q("gmd", "EX_GeographicBoundingBox"))
            for side in ("westBoundLongitude", "eastBoundLongitude", "southBoundLatitude", "northBoundLatitude"):
                side_el = ET.SubElement(bbox, q("gmd", side))
                ET.SubElement(side_el, q("gco", "Decimal")).text = str(geo[side])
            if geo.get("note"):
                geo_el.append(ET.Comment(f" {geo['note']} "))
        temporal = extent.get("temporalElement")
        if temporal and temporal.get("note"):
            ex_extent.append(ET.Comment(f" temporalElement: {temporal['note']} "))

    constraints = ident.get("resourceConstraints")
    if constraints:
        cons_el = ET.SubElement(data_ident, q("gmd", "resourceConstraints"))
        md_cons = ET.SubElement(cons_el, q("gmd", "MD_LegalConstraints"))
        use = constraints.get("useConstraints")
        if use:
            codelist(md_cons, "useConstraints", "MD_RestrictionCode", "license")
            char_string(md_cons, "otherConstraints", f"{use['license']}: {use['licenseUrl']}")
            if use.get("note"):
                md_cons.append(ET.Comment(f" {use['note']} "))
        if constraints.get("otherConstraints"):
            char_string(md_cons, "otherConstraints", constraints["otherConstraints"])

    content = md_json.get("contentInfo", {}).get("featureCatalogue")
    if content:
        ci_el = ET.SubElement(root, q("gmd", "contentInfo"))
        fcd = ET.SubElement(ci_el, q("gmd", "MD_FeatureCatalogueDescription"))
        ft_el = ET.SubElement(fcd, q("gmd", "featureTypes"))
        ET.SubElement(ft_el, q("gco", "LocalName")).text = content["featureType"]
        attr_summary = "; ".join(
            f"{a['name']} ({a['type']}): {a['description']}" for a in content["attributes"]
        )
        fcd.append(ET.Comment(
            " Feature attributes (informative - ISO 19115 contentInfo does not itself carry"
            " per-attribute definitions; see ISO 19110 for a full feature catalogue):"
            f" {attr_summary} "
        ))

    dist = md_json.get("distributionInfo")
    if dist:
        di_el = ET.SubElement(root, q("gmd", "distributionInfo"))
        md_dist = ET.SubElement(di_el, q("gmd", "MD_Distribution"))
        formats = dist.get("distributionFormat", [])
        formats = formats if isinstance(formats, list) else [formats]
        for fmt in formats:
            df_el = ET.SubElement(md_dist, q("gmd", "distributionFormat"))
            md_format = ET.SubElement(df_el, q("gmd", "MD_Format"))
            char_string(md_format, "name", fmt["name"])
            char_string(md_format, "specification", fmt.get("specification", ""))
        distributor = dist.get("distributor")
        if distributor:
            dist_el = ET.SubElement(md_dist, q("gmd", "distributor"))
            md_distributor = ET.SubElement(dist_el, q("gmd", "MD_Distributor"))
            responsible_party(md_distributor, "distributorContact", distributor)
        transfer = dist.get("transferOptions")
        if transfer:
            online_resources = transfer.get("onLine", [])
            if online_resources:
                transfer_el = ET.SubElement(md_dist, q("gmd", "transferOptions"))
                md_transfer = ET.SubElement(transfer_el, q("gmd", "MD_DigitalTransferOptions"))
                for res in online_resources:
                    online_el = ET.SubElement(md_transfer, q("gmd", "onLine"))
                    ci_online = ET.SubElement(online_el, q("gmd", "CI_OnlineResource"))
                    linkage = ET.SubElement(ci_online, q("gmd", "linkage"))
                    ET.SubElement(linkage, q("gmd", "URL")).text = res["url"]
                    if res.get("function"):
                        codelist(ci_online, "function", "CI_OnLineFunctionCode", res["function"])
                    if res.get("name"):
                        char_string(ci_online, "name", res["name"])
                    if res.get("description"):
                        char_string(ci_online, "description", res["description"])
            if transfer.get("note"):
                md_dist.append(ET.Comment(f" transferOptions: {transfer['note']} "))

    dq = md_json.get("dataQualityInfo")
    if dq:
        dq_el = ET.SubElement(root, q("gmd", "dataQualityInfo"))
        dq_dq = ET.SubElement(dq_el, q("gmd", "DQ_DataQuality"))
        scope_el = ET.SubElement(dq_dq, q("gmd", "scope"))
        dq_scope = ET.SubElement(scope_el, q("gmd", "DQ_Scope"))
        codelist(dq_scope, "level", "MD_ScopeCode", dq.get("scope", "dataset"))
        lineage = dq.get("lineage")
        if lineage:
            lin_el = ET.SubElement(dq_dq, q("gmd", "lineage"))
            li_lineage = ET.SubElement(lin_el, q("gmd", "LI_Lineage"))
            char_string(li_lineage, "statement", lineage["statement"])

    return root


def indent(elem, level=0):
    ET.indent(elem, space="  ")


def main():
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "metadata.iso19115.json"
    dst = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(__file__).parent / "metadata.iso19139.xml"

    md_json = json.loads(src.read_text())
    root = build(md_json)
    indent(root)

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    dst.write_bytes(b'<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="utf-8"))
    print(f"Wrote {dst}")


if __name__ == "__main__":
    main()
