import xml.etree.ElementTree as ET
import json
import re
import os

def parse_params(requirements_list):
    required = []
    optional = []
    for req in requirements_list:
        if "Query Parameters Required:" in req:
            parts = req.split("Required: [")[1].split("], Optional: [")
            req_params = parts[0].split(", ") if parts[0] else []
            opt_params = parts[1].split("]")[0].split(", ") if parts[1] != "]" else []
            required.extend([p for p in req_params if p])
            optional.extend([p for p in opt_params if p])
    return required, optional

def parse_sample_url(url_text):
    path_part = url_text.split('datapathdnsnameofappliance')[-1]
    if '?' in path_part:
        pure_path, query_part = path_part.split('?', 1)
    elif '[?' in path_part:
        pure_path, query_part = path_part.split('[?', 1)
    else:
        pure_path = path_part
        query_part = ""
    
    pure_path = pure_path.split('[')[0]
    path = pure_path.replace('{unique identifier or attribute}', '{id}')
    if path.endswith('/') and len(path) > 1:
        path = path[:-1]
    if not path.startswith('/'):
        path = '/' + path
    
    fixed_params = []
    i = 0
    while i < len(query_part):
        if query_part[i] == '[':
            depth = 1
            i += 1
            while i < len(query_part) and depth > 0:
                if query_part[i] == '[': depth += 1
                elif query_part[i] == ']': depth -= 1
                i += 1
        else:
            start = i
            while i < len(query_part) and query_part[i] != '[':
                i += 1
            fixed_part = query_part[start:i]
            parts = [p for p in fixed_part.split('&') if p]
            for p in parts:
                p = re.sub(r'=\{[^}]+\}', '', p).strip()
                if p:
                    fixed_params.append(p)
            
    return path, fixed_params

def guess_type(text):
    if not text: return "string"
    if text.lower() in ["true", "false"]: return "boolean"
    try:
        int(text)
        return "integer"
    except ValueError:
        pass
    return "string"

def infer_schema_from_element(element):
    if len(element) == 0:
        return {"type": guess_type(element.text)}
    
    properties = {}
    required = []
    
    # Check for arrays (multiple children with same tag)
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
    if not s1: return s2
    if not s2: return s1
    if s1["type"] != s2["type"]: return s1 # Conflict, keep first
    
    if s1["type"] == "object":
        res = s1.copy()
        res["properties"] = s1.get("properties", {}).copy()
        s2_props = s2.get("properties", {})
        for k, v in s2_props.items():
            if k in res["properties"]:
                res["properties"][k] = merge_schemas(res["properties"][k], v)
            else:
                res["properties"][k] = v
        return res
    elif s1["type"] == "array":
        res = s1.copy()
        res["items"] = merge_schemas(s1.get("items", {}), s2.get("items", {}))
        return res
    return s1

def main():
    xml_path = 'server/src/main/resources/request-handlers-full.xml'
    # Use a more robust parser for large file with possible encoding issues
    tree = ET.parse(xml_path)
    root = tree.getroot()

    oas = {
        "openapi": "3.0.3",
        "info": {
            "title": "Spectra S3 API",
            "description": "Comprehensive API for Spectra S3 generated from request handlers.",
            "version": "1.0.0"
        },
        "servers": [{"url": "http://datapathdnsnameofappliance"}],
        "paths": {},
        "components": {"schemas": {}}
    }

    schemas = {}

    for handler in root.findall('RequestHandler'):
        handler_name = handler.get('Name')
        doc = handler.find('Documentation').text if handler.find('Documentation') is not None else ""
        req_requirements = [r.text for r in handler.findall('RequestRequirements') if r.text]
        
        is_aws_style = any("Must be an AWS-style request" in r for r in req_requirements)
        has_bucket = any("Must include an S3 bucket specification" in r for r in req_requirements)
        has_object = any("Must include an S3 object specification" in r for r in req_requirements)

        sample_url_tag = handler.find('SampleUrl')
        fixed_params = []
        if sample_url_tag is not None and sample_url_tag.text:
            path, fixed_params = parse_sample_url(sample_url_tag.text)
        elif is_aws_style:
            if has_bucket and has_object: path = "/{bucket}/{object}"
            elif has_bucket: path = "/{bucket}"
            else: path = "/"
        else:
            continue

        method = "get"
        for req in req_requirements:
            if "Must be REST action" in req:
                action = req.split("Must be REST action ")[1]
                method = {"CREATE": "post", "DELETE": "delete", "SHOW": "get", "LIST": "get", "MODIFY": "put", "BULK_MODIFY": "put"}.get(action, "get")
            elif "Must be HTTP request type" in req:
                method = req.split("Must be HTTP request type ")[1].lower()

        required_params, optional_params = parse_params(req_requirements)
        display_path = f"{path}?{'&'.join(sorted(fixed_params))}" if fixed_params else path

        if display_path not in oas["paths"]: oas["paths"][display_path] = {}

        operation = {
            "summary": handler_name.split('.')[-1],
            "description": doc,
            "operationId": handler_name,
            "tags": [handler_name.split('.')[-2]],
            "parameters": [],
            "responses": {}
        }

        for p in re.findall(r'\{([^}]+)\}', path):
            operation["parameters"].append({"name": p, "in": "path", "required": True, "schema": {"type": "string"}})

        for p in required_params:
            if any(par["name"] == p for par in operation["parameters"]): continue
            default_val = next((fp.split('=')[1] if '=' in fp else "" for fp in fixed_params if fp == p or fp.startswith(p+'=')), None)
            param = {"name": p, "in": "query", "required": True, "schema": {"type": "string"}}
            if default_val is not None: param["schema"]["default"] = default_val
            operation["parameters"].append(param)

        for p in optional_params:
            if any(par["name"] == p for par in operation["parameters"]): continue
            operation["parameters"].append({"name": p, "in": "query", "required": False, "schema": {"type": "string"}})

        for sample_res in handler.findall('SampleResponses'):
            code = sample_res.find('HttpResponseCode').text or "200"
            res_type_full = sample_res.find('HttpResponseType').text
            res_text = sample_res.find('HttpResponse').text if sample_res.find('HttpResponse') is not None else ""
            
            if code not in operation["responses"]:
                operation["responses"][code] = {"description": f"Response for {code}"}
            
            if res_type_full and res_type_full != "null":
                schema_id = res_type_full.split('.')[-1].replace(';', '').replace('[', 'List')
                
                # Try to extract XML from HttpResponse
                xml_match = re.search(r'(<[^>]+>.*</[^>]+>)', res_text, re.DOTALL)
                if xml_match:
                    xml_str = xml_match.group(1)
                    try:
                        sample_root = ET.fromstring(xml_str)
                        inferred = infer_schema_from_element(sample_root)
                        inferred["xml"] = {"name": sample_root.tag}
                        schemas[schema_id] = merge_schemas(schemas.get(schema_id), inferred)
                    except ET.ParseError:
                        pass
                
                operation["responses"][code]["content"] = {
                    "application/xml": {"schema": {"$ref": f"#/components/schemas/{schema_id}"}}
                }
                if schema_id not in schemas: schemas[schema_id] = {"type": "object"}

        if method in oas["paths"][display_path]:
            if 'spectrads3' in handler_name: oas["paths"][display_path][method] = operation
        else:
            oas["paths"][display_path][method] = operation

    oas["components"]["schemas"] = schemas
    with open('openapi.json', 'w') as f:
        json.dump(oas, f, indent=2)

if __name__ == "__main__":
    main()
