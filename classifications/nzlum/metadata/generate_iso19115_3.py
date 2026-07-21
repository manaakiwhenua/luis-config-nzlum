#!/usr/bin/env python3
"""Generate an ISO/TS 19115-3 (mdb/cit/mri/...) XML metadata record from metadata.iso19115.json.

metadata.iso19115.json is the single source of truth for this dataset's
discovery/catalogue metadata. This script derives metadata.iso19115-3.xml from
it, in the newer package-based XML encoding of ISO 19115 (as opposed to
generate_iso19139.py, which derives the older, still more widely-supported
ISO 19139 encoding from the same JSON). The XML is a build artefact and should
not be hand-edited - re-run this script after changing the JSON.

This mirrors the major structural differences between the two encodings
(package-per-namespace, CI_Responsibility/party replacing CI_ResponsibleParty,
defaultLocale replacing flat language/characterSet, metadataScope replacing
flat hierarchyLevel, dateInfo replacing flat dateStamp, metadataStandard
replacing flat metadataStandardName/Version) but, like generate_iso19139.py,
is a best-effort informative rendering rather than a schema-validated one -
a few less load-bearing package boundaries (e.g. around MD_Format, DQ_Scope)
are approximate.

Usage:
    python3 generate_iso19115_3.py [metadata.iso19115.json] [metadata.iso19115-3.xml]
"""
import json
import sys
from pathlib import Path
from xml.etree import ElementTree as ET

NS = {
    "mdb": "http://standards.iso.org/iso/19115/-3/mdb/2.0",
    "cit": "http://standards.iso.org/iso/19115/-3/cit/2.0",
    "gex": "http://standards.iso.org/iso/19115/-3/gex/1.0",
    "mri": "http://standards.iso.org/iso/19115/-3/mri/1.0",
    "mrd": "http://standards.iso.org/iso/19115/-3/mrd/1.0",
    "mdq": "http://standards.iso.org/iso/19115/-3/mdq/1.0",
    "mrl": "http://standards.iso.org/iso/19115/-3/mrl/2.0",
    "mrc": "http://standards.iso.org/iso/19115/-3/mrc/2.0",
    "mco": "http://standards.iso.org/iso/19115/-3/mco/1.0",
    "mcc": "http://standards.iso.org/iso/19115/-3/mcc/1.0",
    "msr": "http://standards.iso.org/iso/19115/-3/msr/2.0",
    "mrs": "http://standards.iso.org/iso/19115/-3/mrs/1.0",
    "mmi": "http://standards.iso.org/iso/19115/-3/mmi/1.0",
    "lan": "http://standards.iso.org/iso/19115/-3/lan/1.0",
    "gco": "http://standards.iso.org/iso/19115/-3/gco/1.0",
    "gml": "http://www.opengis.net/gml/3.2",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}
CODELIST_BASE = "https://standards.iso.org/iso/19115/resources/Codelist/cat/codelists.xml"

for prefix, uri in NS.items():
    ET.register_namespace(prefix, uri)


def q(prefix, tag):
    return f"{{{NS[prefix]}}}{tag}"


def char_string(parent, ns, tag, text):
    el = ET.SubElement(parent, q(ns, tag))
    cs = ET.SubElement(el, q("gco", "CharacterString"))
    cs.text = text
    return el


def codelist(parent, ns, tag, code_ns, codelist_name, value, text=None):
    el = ET.SubElement(parent, q(ns, tag))
    code_el = ET.SubElement(
        el,
        q(code_ns, codelist_name),
        {
            "codeList": f"{CODELIST_BASE}#{codelist_name}",
            "codeListValue": value,
        },
    )
    code_el.text = text if text is not None else value
    return el


def md_identifier(parent, ns, tag, code, code_space=None, comment=None):
    el = ET.SubElement(parent, q(ns, tag))
    identifier = ET.SubElement(el, q("mcc", "MD_Identifier"))
    char_string(identifier, "mcc", "code", code)
    if code_space:
        char_string(identifier, "mcc", "codeSpace", code_space)
    if comment:
        el.append(ET.Comment(f" {comment} "))
    return el


def _add_contact(parent, email):
    contact_info = ET.SubElement(parent, q("cit", "contactInfo"))
    ci = ET.SubElement(contact_info, q("cit", "CI_Contact"))
    address_el = ET.SubElement(ci, q("cit", "address"))
    address = ET.SubElement(address_el, q("cit", "CI_Address"))
    char_string(address, "cit", "electronicMailAddress", email)


def responsibility(parent, ns, tag, party):
    """Build a CI_Responsibility (party model) - replaces 19139's flat CI_ResponsibleParty."""
    el = ET.SubElement(parent, q(ns, tag))
    resp = ET.SubElement(el, q("cit", "CI_Responsibility"))
    codelist(resp, "cit", "role", "cit", "CI_RoleCode", party.get("role", "pointOfContact"))
    party_el = ET.SubElement(resp, q("cit", "party"))
    if party.get("individualName"):
        indiv = ET.SubElement(party_el, q("cit", "CI_Individual"))
        char_string(indiv, "cit", "name", party["individualName"])
        if party.get("positionName"):
            char_string(indiv, "cit", "positionName", party["positionName"])
        if party.get("organisationName"):
            org_wrap = ET.SubElement(indiv, q("cit", "partOfOrganisation"))
            org = ET.SubElement(org_wrap, q("cit", "CI_Organisation"))
            char_string(org, "cit", "name", party["organisationName"])
        if party.get("electronicMailAddress"):
            _add_contact(indiv, party["electronicMailAddress"])
    elif party.get("organisationName"):
        org = ET.SubElement(party_el, q("cit", "CI_Organisation"))
        char_string(org, "cit", "name", party["organisationName"])
        if party.get("electronicMailAddress"):
            _add_contact(org, party["electronicMailAddress"])
    return el


def build(md_json):
    root = ET.Element(q("mdb", "MD_Metadata"))

    md_identifier(root, "mdb", "metadataIdentifier", md_json["fileIdentifier"])

    locale_el = ET.SubElement(root, q("mdb", "defaultLocale"))
    pt_locale = ET.SubElement(locale_el, q("lan", "PT_Locale"))
    codelist(pt_locale, "lan", "language", "lan", "LanguageCode", md_json.get("language", "eng"))
    codelist(pt_locale, "lan", "characterEncoding", "lan", "MD_CharacterSetCode", md_json.get("characterSet", "utf8"))

    scope_el = ET.SubElement(root, q("mdb", "metadataScope"))
    md_scope = ET.SubElement(scope_el, q("mdb", "MD_MetadataScope"))
    codelist(md_scope, "mdb", "resourceScope", "mcc", "MD_ScopeCode", md_json.get("hierarchyLevel", "dataset"))

    for contact in md_json.get("contact", []):
        responsibility(root, "mdb", "contact", contact)

    date_info_el = ET.SubElement(root, q("mdb", "dateInfo"))
    ci_date = ET.SubElement(date_info_el, q("cit", "CI_Date"))
    date_val = ET.SubElement(ci_date, q("cit", "date"))
    ET.SubElement(date_val, q("gco", "Date")).text = md_json["dateStamp"]
    codelist(ci_date, "cit", "dateType", "cit", "CI_DateTypeCode", "creation")

    std_el = ET.SubElement(root, q("mdb", "metadataStandard"))
    std_citation = ET.SubElement(std_el, q("cit", "CI_Citation"))
    char_string(std_citation, "cit", "title", md_json.get("metadataStandardName", "ISO 19115"))
    if md_json.get("metadataStandardVersion"):
        char_string(std_citation, "cit", "edition", md_json["metadataStandardVersion"])

    for rsi in md_json.get("referenceSystemInfo", []):
        rsi_el = ET.SubElement(root, q("mdb", "referenceSystemInfo"))
        mrs_rs = ET.SubElement(rsi_el, q("mrs", "MD_ReferenceSystem"))
        rs_id_el = ET.SubElement(mrs_rs, q("mrs", "referenceSystemIdentifier"))
        identifier = ET.SubElement(rs_id_el, q("mcc", "MD_Identifier"))
        char_string(identifier, "mcc", "code", rsi["code"])
        char_string(identifier, "mcc", "codeSpace", rsi.get("codeSpace", ""))
        if rsi.get("note"):
            rsi_el.append(ET.Comment(f" {rsi['name']}: {rsi['note']} "))

    spatial_rep = md_json.get("spatialRepresentationInfo")
    if spatial_rep:
        sri_el = ET.SubElement(root, q("mdb", "spatialRepresentationInfo"))
        vsr = ET.SubElement(sri_el, q("msr", "MD_VectorSpatialRepresentation"))
        codelist(vsr, "msr", "topologyLevel", "msr", "MD_TopologyLevelCode", spatial_rep.get("topologyLevel", "geometryOnly"))
        geom_obj = ET.SubElement(vsr, q("msr", "geometricObjects"))
        mgo = ET.SubElement(geom_obj, q("msr", "MD_GeometricObjects"))
        codelist(mgo, "msr", "geometricObjectType", "msr", "MD_GeometricObjectTypeCode", "surface")
        dggs = spatial_rep.get("dggs")
        if dggs:
            sri_el.append(ET.Comment(
                f" DGGS: {dggs['system']}, resolution {dggs['resolution']}"
                f" (avg. edge {dggs['averageEdgeLengthMetres']} m,"
                f" avg. cell area {dggs['averageCellAreaKm2']} km2)."
                f" {dggs.get('note', '')} "
            ))

    ident = md_json["identificationInfo"]
    ident_el = ET.SubElement(root, q("mdb", "identificationInfo"))
    data_ident = ET.SubElement(ident_el, q("mri", "MD_DataIdentification"))

    citation_el = ET.SubElement(data_ident, q("mri", "citation"))
    ci_citation = ET.SubElement(citation_el, q("cit", "CI_Citation"))
    cit = ident["citation"]
    char_string(ci_citation, "cit", "title", cit["title"])
    if cit.get("alternateTitle"):
        char_string(ci_citation, "cit", "alternateTitle", cit["alternateTitle"])
    dates = cit["date"] if isinstance(cit["date"], list) else [cit["date"]]
    for date in dates:
        cd = ET.SubElement(ci_citation, q("cit", "date"))
        ci_date2 = ET.SubElement(cd, q("cit", "CI_Date"))
        date_val2 = ET.SubElement(ci_date2, q("cit", "date"))
        ET.SubElement(date_val2, q("gco", "Date")).text = date["value"]
        codelist(ci_date2, "cit", "dateType", "cit", "CI_DateTypeCode", date["type"])
        if date.get("note"):
            cd.append(ET.Comment(f" {date['note']} "))
    char_string(ci_citation, "cit", "edition", cit["edition"])
    ed_date = ET.SubElement(ci_citation, q("cit", "editionDate"))
    ET.SubElement(ed_date, q("gco", "Date")).text = cit["editionDate"]
    for party in cit.get("citedResponsibleParty", []):
        responsibility(ci_citation, "cit", "citedResponsibility", party)
    identifiers = cit.get("identifier", [])
    identifiers = identifiers if isinstance(identifiers, list) else [identifiers]
    for identifier in identifiers:
        md_identifier(ci_citation, "cit", "identifier", identifier["code"],
                      identifier.get("codeSpace", ""), identifier.get("description"))
    if cit.get("otherCitationDetails"):
        char_string(ci_citation, "cit", "otherCitationDetails", cit["otherCitationDetails"])

    char_string(data_ident, "mri", "abstract", ident["abstract"])
    if ident.get("purpose"):
        char_string(data_ident, "mri", "purpose", ident["purpose"])
    for party in ident.get("pointOfContact", []):
        responsibility(data_ident, "mri", "pointOfContact", party)
    codelist(data_ident, "mri", "status", "mri", "MD_ProgressCode", ident.get("status", "onGoing"))

    maint = ident.get("resourceMaintenance")
    if maint:
        maint_el = ET.SubElement(data_ident, q("mri", "resourceMaintenance"))
        mmi_info = ET.SubElement(maint_el, q("mmi", "MD_MaintenanceInformation"))
        codelist(mmi_info, "mmi", "maintenanceAndUpdateFrequency", "mmi", "MD_MaintenanceFrequencyCode",
                 maint.get("maintenanceAndUpdateFrequency", "asNeeded"))
        if maint.get("note"):
            char_string(mmi_info, "mmi", "maintenanceNote", maint["note"])

    for kw_group in ident.get("descriptiveKeywords", []):
        dk_el = ET.SubElement(data_ident, q("mri", "descriptiveKeywords"))
        md_kw = ET.SubElement(dk_el, q("mri", "MD_Keywords"))
        for kw in kw_group["keywords"]:
            char_string(md_kw, "mri", "keyword", kw)
        codelist(md_kw, "mri", "type", "mri", "MD_KeywordTypeCode", kw_group["type"])

    res = ident.get("spatialResolution")
    if res:
        res_el = ET.SubElement(data_ident, q("mri", "spatialResolution"))
        md_res = ET.SubElement(res_el, q("mri", "MD_Resolution"))
        dist_el = ET.SubElement(md_res, q("mri", "distance"))
        ET.SubElement(dist_el, q("gco", "Distance"), {"uom": res["distance"]["uom"]}).text = str(res["distance"]["value"])
        if res.get("note"):
            res_el.append(ET.Comment(f" {res['note']} "))

    for topic in ident.get("topicCategory", []):
        tc_el = ET.SubElement(data_ident, q("mri", "topicCategory"))
        ET.SubElement(tc_el, q("mri", "MD_TopicCategoryCode")).text = topic

    extent = ident.get("extent")
    if extent:
        extent_el = ET.SubElement(data_ident, q("mri", "extent"))
        ex_extent = ET.SubElement(extent_el, q("gex", "EX_Extent"))
        geo = extent.get("geographicElement")
        if geo:
            geo_el = ET.SubElement(ex_extent, q("gex", "geographicElement"))
            bbox = ET.SubElement(geo_el, q("gex", "EX_GeographicBoundingBox"))
            for side in ("westBoundLongitude", "eastBoundLongitude", "southBoundLatitude", "northBoundLatitude"):
                side_el = ET.SubElement(bbox, q("gex", side))
                ET.SubElement(side_el, q("gco", "Decimal")).text = str(geo[side])
            if geo.get("note"):
                geo_el.append(ET.Comment(f" {geo['note']} "))
        temporal = extent.get("temporalElement")
        if temporal and temporal.get("note"):
            ex_extent.append(ET.Comment(f" temporalElement: {temporal['note']} "))

    constraints = ident.get("resourceConstraints")
    if constraints:
        cons_el = ET.SubElement(data_ident, q("mri", "resourceConstraints"))
        md_cons = ET.SubElement(cons_el, q("mco", "MD_LegalConstraints"))
        use = constraints.get("useConstraints")
        if use:
            codelist(md_cons, "mco", "useConstraints", "mco", "MD_RestrictionCode", "license")
            char_string(md_cons, "mco", "otherConstraints", f"{use['license']}: {use['licenseUrl']}")
            if use.get("note"):
                md_cons.append(ET.Comment(f" {use['note']} "))
        if constraints.get("otherConstraints"):
            char_string(md_cons, "mco", "otherConstraints", constraints["otherConstraints"])

    content = md_json.get("contentInfo", {}).get("featureCatalogue")
    if content:
        ci_el = ET.SubElement(root, q("mdb", "contentInfo"))
        fcd = ET.SubElement(ci_el, q("mrc", "MD_FeatureCatalogueDescription"))
        ft_el = ET.SubElement(fcd, q("mrc", "featureTypes"))
        fti = ET.SubElement(ft_el, q("mrc", "MD_FeatureTypeInfo"))
        ET.SubElement(fti, q("mrc", "featureTypeName")).text = content["featureType"]
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
        di_el = ET.SubElement(root, q("mdb", "distributionInfo"))
        md_dist = ET.SubElement(di_el, q("mrd", "MD_Distribution"))
        formats = dist.get("distributionFormat", [])
        formats = formats if isinstance(formats, list) else [formats]
        for fmt in formats:
            df_el = ET.SubElement(md_dist, q("mrd", "distributionFormat"))
            md_format = ET.SubElement(df_el, q("mrd", "MD_Format"))
            fmt_citation_el = ET.SubElement(md_format, q("mrd", "formatSpecificationCitation"))
            fmt_citation = ET.SubElement(fmt_citation_el, q("cit", "CI_Citation"))
            char_string(fmt_citation, "cit", "title", fmt["name"])
            if fmt.get("specification"):
                char_string(fmt_citation, "cit", "alternateTitle", fmt["specification"])
        distributor = dist.get("distributor")
        if distributor:
            dist_el = ET.SubElement(md_dist, q("mrd", "distributor"))
            md_distributor = ET.SubElement(dist_el, q("mrd", "MD_Distributor"))
            responsibility(md_distributor, "mrd", "distributorContact", distributor)
        transfer = dist.get("transferOptions")
        if transfer:
            online_resources = transfer.get("onLine", [])
            if online_resources:
                transfer_el = ET.SubElement(md_dist, q("mrd", "transferOptions"))
                md_transfer = ET.SubElement(transfer_el, q("mrd", "MD_DigitalTransferOptions"))
                for res in online_resources:
                    online_el = ET.SubElement(md_transfer, q("mrd", "onLine"))
                    ci_online = ET.SubElement(online_el, q("cit", "CI_OnlineResource"))
                    linkage = ET.SubElement(ci_online, q("cit", "linkage"))
                    ET.SubElement(linkage, q("gco", "CharacterString")).text = res["url"]
                    if res.get("function"):
                        codelist(ci_online, "cit", "function", "cit", "CI_OnLineFunctionCode", res["function"])
                    if res.get("name"):
                        char_string(ci_online, "cit", "name", res["name"])
                    if res.get("description"):
                        char_string(ci_online, "cit", "description", res["description"])
            if transfer.get("note"):
                md_dist.append(ET.Comment(f" transferOptions: {transfer['note']} "))

    dq = md_json.get("dataQualityInfo")
    if dq:
        dq_el = ET.SubElement(root, q("mdb", "dataQualityInfo"))
        dq_dq = ET.SubElement(dq_el, q("mdq", "DQ_DataQuality"))
        scope_el2 = ET.SubElement(dq_dq, q("mdq", "scope"))
        dq_scope = ET.SubElement(scope_el2, q("mcc", "MD_Scope"))
        codelist(dq_scope, "mcc", "level", "mcc", "MD_ScopeCode", dq.get("scope", "dataset"))
        lineage = dq.get("lineage")
        if lineage:
            lin_el = ET.SubElement(dq_dq, q("mdq", "lineage"))
            li_lineage = ET.SubElement(lin_el, q("mrl", "LI_Lineage"))
            char_string(li_lineage, "mrl", "statement", lineage["statement"])

    return root


def main():
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "metadata.iso19115.json"
    dst = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(__file__).parent / "metadata.iso19115-3.xml"

    md_json = json.loads(src.read_text())
    root = build(md_json)

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    dst.write_bytes(b'<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="utf-8"))
    print(f"Wrote {dst}")


if __name__ == "__main__":
    main()
