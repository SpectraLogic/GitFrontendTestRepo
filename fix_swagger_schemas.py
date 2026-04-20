"""
Fix response schemas in swagger.json by re-inferring them from
the SampleResponses in request-handlers-full.xml.

Reads swagger.json, rebuilds all component schemas from the XML
sample responses, and writes the fixed result back to swagger.json.

Usage:
    python fix_swagger_schemas.py
"""

import xml.etree.ElementTree as ET
import json
import re


def guess_type(text):
    if not text:
        return "string"
    if text.lower() in ("true", "false"):
        return "boolean"
    try:
        int(text)
        return "integer"
    except ValueError:
        pass
    try:
        float(text)
        return "number"
    except ValueError:
        pass
    return "string"


def infer_schema_from_element(element):
    """Infer a JSON Schema object from an XML element tree."""
    if len(element) == 0:
        return {"type": guess_type(element.text)}

    properties = {}
    required = []

    child_tags = [child.tag for child in element]
    tag_counts = {tag: child_tags.count(tag) for tag in set(child_tags)}

    for child in element:
        tag = child.tag
        if tag_counts[tag] > 1:
            if tag not in properties:
                properties[tag] = {
                    "type": "array",
                    "items": infer_schema_from_element(child),
                    "xml": {"name": tag, "wrapped": False}
                }
        else:
            properties[tag] = infer_schema_from_element(child)
            properties[tag]["xml"] = {"name": tag}
            required.append(tag)

    schema = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def merge_schemas(s1, s2):
    """Merge two schemas, combining properties from both."""
    if not s1:
        return s2
    if not s2:
        return s1
    if s1.get("type") != s2.get("type"):
        return s1

    if s1["type"] == "object":
        res = dict(s1)
        res["properties"] = dict(s1.get("properties", {}))
        for k, v in s2.get("properties", {}).items():
            if k in res["properties"]:
                res["properties"][k] = merge_schemas(res["properties"][k], v)
            else:
                res["properties"][k] = v
        # Union of required fields from both, but only if the field exists
        all_required = set(s1.get("required", [])) | set(s2.get("required", []))
        if all_required:
            res["required"] = sorted(all_required)
        return res
    elif s1["type"] == "array":
        res = dict(s1)
        res["items"] = merge_schemas(s1.get("items", {}), s2.get("items", {}))
        return res
    return s1


def build_schemas_from_xml(xml_path):
    """Parse request-handlers-full.xml and infer schemas from SampleResponses."""
    tree = ET.parse(xml_path)
    root = tree.getroot()

    schemas = {}

    for handler in root.findall("RequestHandler"):
        for sample_res in handler.findall("SampleResponses"):
            res_type_el = sample_res.find("HttpResponseType")
            res_type_full = res_type_el.text if res_type_el is not None else None
            http_response_el = sample_res.find("HttpResponse")
            res_text = http_response_el.text if http_response_el is not None else ""

            if not res_type_full or res_type_full == "null":
                continue

            schema_id = res_type_full.split(".")[-1].replace(";", "").replace("[", "List")

            # Extract XML body from HttpResponse text
            xml_match = re.search(r"(<[^>]+>.*</[^>]+>)", res_text, re.DOTALL)
            if xml_match:
                xml_str = xml_match.group(1)
                try:
                    sample_root = ET.fromstring(xml_str)
                    inferred = infer_schema_from_element(sample_root)
                    inferred["xml"] = {"name": sample_root.tag}
                    schemas[schema_id] = merge_schemas(schemas.get(schema_id), inferred)
                except ET.ParseError:
                    pass

            # Fallback for schemas with no parseable XML
            if schema_id not in schemas:
                schemas[schema_id] = {"type": "object"}

    return schemas


def main():
    swagger_path = "swagger.json"
    xml_path = "server/src/main/resources/request-handlers-full.xml"

    with open(swagger_path) as f:
        swagger = json.load(f)

    schemas = build_schemas_from_xml(xml_path)

    # Preserve any schemas in swagger.json that aren't in the XML
    # (shouldn't happen, but be safe)
    existing = swagger.get("components", {}).get("schemas", {})
    for schema_id, schema in existing.items():
        if schema_id not in schemas:
            schemas[schema_id] = schema

    swagger.setdefault("components", {})["schemas"] = schemas

    with open(swagger_path, "w") as f:
        json.dump(swagger, f, indent=2)

    # Stats
    total = len(schemas)
    with_props = sum(1 for s in schemas.values() if "properties" in s)
    print(f"Fixed {swagger_path}: {with_props}/{total} schemas now have properties")


if __name__ == "__main__":
    main()
