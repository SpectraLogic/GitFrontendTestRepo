"""
Generate an OpenAPI 3.0.3 spec from request-handlers-full.xml with fully
inferred response schemas, enriched by Java interface definitions.

Combines:
  1. Path/operation generation from the XML request handlers
  2. Schema inference from XML SampleResponses
  3. Schema enrichment from Java domain interfaces in common/src and server/src

Usage:
    python generate_swagger.py [path/to/request-handlers-full.xml]

Output:
    xm-api-swagger.json
"""

import xml.etree.ElementTree as ET
import json
import os
import re
import sys


# ---------------------------------------------------------------------------
# Java type -> JSON Schema mapping
# ---------------------------------------------------------------------------

JAVA_TYPE_MAP = {
    "String":  {"type": "string"},
    "UUID":    {"type": "string", "format": "uuid"},
    "Date":    {"type": "string", "format": "date-time"},
    "Long":    {"type": "integer", "format": "int64"},
    "long":    {"type": "integer", "format": "int64"},
    "Integer": {"type": "integer", "format": "int32"},
    "int":     {"type": "integer", "format": "int32"},
    "Boolean": {"type": "boolean"},
    "boolean": {"type": "boolean"},
    "Double":  {"type": "number", "format": "double"},
    "double":  {"type": "number", "format": "double"},
    "Float":   {"type": "number", "format": "float"},
    "float":   {"type": "number", "format": "float"},
}


def java_type_to_schema(java_type, known_schemas, enum_values_map=None):
    """Convert a Java return type to a JSON Schema fragment."""
    # Array types: Type[] or List<Type>
    array_match = re.match(r"(\w+)\[\]$", java_type)
    if not array_match:
        array_match = re.match(r"(?:List|Collection|Set)<(\w+)>$", java_type)
    if array_match:
        inner = array_match.group(1)
        return {"type": "array", "items": java_type_to_schema(inner, known_schemas, enum_values_map)}

    # Primitive / well-known types
    if java_type in JAVA_TYPE_MAP:
        return dict(JAVA_TYPE_MAP[java_type])

    # Enum types — include enum values if available
    if java_type in known_schemas:
        if enum_values_map and java_type in enum_values_map:
            return {"type": "string", "enum": enum_values_map[java_type]}
        return {"type": "string"}

    # Unknown domain types — treat as string (conservative)
    return {"type": "string"}


# ---------------------------------------------------------------------------
# Java interface parser
# ---------------------------------------------------------------------------

# Matches: ReturnType getFieldName() or boolean isFieldName()
_GETTER_RE = re.compile(
    r"^\s*(?:@\w+(?:\([^)]*\))?\s*)*"        # optional annotations
    r"([\w<>\[\],\s]+?)\s+"                    # return type (group 1)
    r"(get|is)(\w+)\s*\(\s*\)\s*;",           # get/is + Name (groups 2, 3)
    re.MULTILINE
)

# Matches: public interface Foo extends Bar, Baz<Qux> {
# Note: we don't use a single regex for the full declaration because type
# parameters can contain nested < > (e.g., Enum< E >).  Instead we find
# the interface keyword, then manually skip balanced angle brackets.
_INTERFACE_START_RE = re.compile(r"public\s+interface\s+(\w+)")

# Matches: @Optional on the line(s) before a getter
_OPTIONAL_RE = re.compile(r"@Optional")


def parse_java_interface(file_path):
    """Parse a single Java interface file, returning (name, parents, fields).

    fields is a list of (field_name, java_return_type, is_optional).
    """
    with open(file_path) as f:
        source = f.read()

    # Strip block comments
    source = re.sub(r"/\*.*?\*/", "", source, flags=re.DOTALL)
    # Strip line comments
    source = re.sub(r"//[^\n]*", "", source)

    m = _INTERFACE_START_RE.search(source)
    if not m:
        return None, [], []

    iface_name = m.group(1)

    # Skip past optional type parameters (balanced < >) after the name
    pos = m.end()
    while pos < len(source) and source[pos] in " \t\n\r":
        pos += 1
    if pos < len(source) and source[pos] == "<":
        depth = 1
        pos += 1
        while pos < len(source) and depth > 0:
            if source[pos] == "<":
                depth += 1
            elif source[pos] == ">":
                depth -= 1
            pos += 1

    # Look for "extends" clause up to the opening "{"
    brace_pos = source.find("{", pos)
    between = source[pos:brace_pos] if brace_pos != -1 else source[pos:]
    extends_match = re.search(r"\bextends\s+(.*)", between, re.DOTALL)
    parents_raw = extends_match.group(1).strip() if extends_match else ""

    # Parse parent list, stripping type parameters
    parents = []
    if parents_raw:
        # Split on commas that are not inside < >
        depth = 0
        current = []
        for ch in parents_raw:
            if ch == "<":
                depth += 1
            elif ch == ">":
                depth -= 1
            elif ch == "," and depth == 0:
                parents.append("".join(current).strip())
                current = []
                continue
            current.append(ch)
        if current:
            parents.append("".join(current).strip())
        # Strip type params from parent names and get simple name
        parents = [re.sub(r"<.*", "", p).strip().split(".")[-1] for p in parents]

    # Extract getters
    fields = []
    for gm in _GETTER_RE.finditer(source):
        return_type = gm.group(1).strip()
        prefix = gm.group(2)          # "get" or "is"
        name_part = gm.group(3)       # e.g., "Name", "Protected"
        field_name = name_part        # already PascalCase

        # Check for @Optional in the ~200 chars before this getter
        start = max(0, gm.start() - 200)
        preceding = source[start:gm.start()]
        is_optional = bool(_OPTIONAL_RE.search(preceding))

        fields.append((field_name, return_type, is_optional))

    return iface_name, parents, fields


def scan_java_interfaces(base_dirs):
    """Scan directories for Java interfaces and return a dict of
    interface_name -> {parents: [...], fields: [(name, type, optional), ...], file: path}.
    """
    registry = {}

    for base_dir in base_dirs:
        if not os.path.isdir(base_dir):
            continue
        for dirpath, _, filenames in os.walk(base_dir):
            for fname in filenames:
                if not fname.endswith(".java"):
                    continue
                fpath = os.path.join(dirpath, fname)
                iface_name, parents, fields = parse_java_interface(fpath)
                if iface_name:
                    if iface_name not in registry or fields:
                        registry[iface_name] = {
                            "parents": parents,
                            "fields": fields,
                            "file": fpath,
                        }

    return registry


def resolve_all_fields(iface_name, registry, _seen=None):
    """Recursively resolve all fields for an interface, including inherited ones."""
    if _seen is None:
        _seen = set()
    if iface_name in _seen:
        return []
    _seen.add(iface_name)

    entry = registry.get(iface_name)
    if not entry:
        return []

    all_fields = list(entry["fields"])
    for parent in entry["parents"]:
        all_fields.extend(resolve_all_fields(parent, registry, _seen))

    return all_fields


def build_java_schema(iface_name, registry, known_schemas, enum_values_map=None):
    """Build a JSON Schema for a Java interface using its field definitions."""
    fields = resolve_all_fields(iface_name, registry)
    if not fields:
        return None

    # Deduplicate by field name (first occurrence wins — child overrides parent)
    seen = set()
    unique_fields = []
    for f in fields:
        if f[0] not in seen:
            seen.add(f[0])
            unique_fields.append(f)

    properties = {}
    required = []

    for field_name, java_type, is_optional in unique_fields:
        prop = java_type_to_schema(java_type, known_schemas, enum_values_map)
        properties[field_name] = prop
        if not is_optional:
            required.append(field_name)

    schema = {"type": "object", "properties": properties}
    if required:
        schema["required"] = sorted(required)
    return schema


# ---------------------------------------------------------------------------
# XML Schema inference helpers
# ---------------------------------------------------------------------------

def guess_type(text):
    """Guess JSON Schema type from an XML text value."""
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
        all_required = set(s1.get("required", [])) | set(s2.get("required", []))
        if all_required:
            res["required"] = sorted(all_required)
        # Preserve xml name from whichever side has it
        if "xml" in s1:
            res["xml"] = s1["xml"]
        elif "xml" in s2:
            res["xml"] = s2["xml"]
        return res
    elif s1["type"] == "array":
        res = dict(s1)
        res["items"] = merge_schemas(s1.get("items", {}), s2.get("items", {}))
        return res
    return s1


def parse_contract_param_types(contract_xml_path):
    """Parse request-handlers-contract.xml and return a dict of
    handler_name -> {lowercase_param_name -> (canonical_name, java_type_string)}.

    canonical_name is the PascalCase name from the contract XML (e.g. "DataPolicyId").
    """
    if not os.path.isfile(contract_xml_path):
        print(f"  [skip] {contract_xml_path} not found, parameter types will default to string")
        return {}

    tree = ET.parse(contract_xml_path)
    root = tree.getroot()

    handler_params = {}
    for handler in root.findall(".//RequestHandler"):
        name = handler.get("Name")
        if not name:
            continue
        params = {}
        request = handler.find("Request")
        if request is None:
            continue
        for section in ("RequiredQueryParams", "OptionalQueryParams"):
            section_el = request.find(section)
            if section_el is None:
                continue
            for param in section_el.findall("Param"):
                pname = param.get("Name")
                ptype = param.get("Type")
                if pname and ptype:
                    params[pname.lower()] = (pname, ptype)
        if params:
            handler_params[name] = params

    return handler_params


def parse_enum_values(source):
    """Extract enum constant names from Java enum source code."""
    # Strip comments
    source = re.sub(r"/\*.*?\*/", "", source, flags=re.DOTALL)
    source = re.sub(r"//[^\n]*", "", source)

    # Find the enum body between the first { and its matching }
    m = re.search(r"\bpublic\s+enum\s+\w+[^{]*\{", source)
    if not m:
        return []

    brace_start = m.end()
    # Enum constants end at the first ; inside the enum body
    semi_pos = source.find(";", brace_start)
    if semi_pos == -1:
        # No semicolon — enum body extends to closing brace
        depth = 1
        pos = brace_start
        while pos < len(source) and depth > 0:
            if source[pos] == "{":
                depth += 1
            elif source[pos] == "}":
                depth -= 1
            pos += 1
        constants_text = source[brace_start:pos - 1]
    else:
        constants_text = source[brace_start:semi_pos]

    # Parse constant names: identifiers before optional ( or , or end
    # Skip anything inside parentheses (constructor args)
    values = []
    depth = 0
    token = []
    for ch in constants_text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0:
            if ch == "," or ch == "\n":
                name = "".join(token).strip()
                if name and re.match(r"^[A-Z_][A-Z_0-9]*$", name):
                    values.append(name)
                token = []
                continue
            token.append(ch)
    # Last token
    name = "".join(token).strip()
    if name and re.match(r"^[A-Z_][A-Z_0-9]*$", name):
        values.append(name)

    return values


def contract_type_to_schema(java_type, enum_values_map=None):
    """Convert a Java type from the contract XML to a JSON Schema fragment.

    Handles fully-qualified names (e.g. java.util.UUID, com.spectralogic...WriteOptimization)
    as well as primitives (long, int, boolean, double).
    Returns only the base type (no format qualifier).
    """
    type_map = {
        "long":               {"type": "integer", "format": "int64"},
        "int":                {"type": "integer", "format": "int32"},
        "boolean":            {"type": "boolean"},
        "double":             {"type": "number", "format": "double"},
        "float":              {"type": "number", "format": "float"},
        "Long":               {"type": "integer", "format": "int64"},
        "Integer":            {"type": "integer", "format": "int32"},
        "Boolean":            {"type": "boolean"},
        "Double":             {"type": "number", "format": "double"},
        "Float":              {"type": "number", "format": "float"},
        "String":             {"type": "string"},
        "java.lang.Long":     {"type": "integer", "format": "int64"},
        "java.lang.Integer":  {"type": "integer", "format": "int32"},
        "java.lang.Double":   {"type": "number", "format": "double"},
        "java.lang.Boolean":  {"type": "boolean"},
        "java.lang.String":   {"type": "string"},
        "java.util.UUID":     {"type": "string"},
        "java.util.Date":     {"type": "string", "format": "date-time"},
    }

    if java_type in type_map:
        return dict(type_map[java_type])

    # Strip FQN to simple name and check again
    simple = java_type.rsplit(".", 1)[-1] if "." in java_type else java_type
    if simple in type_map:
        return dict(type_map[simple])

    # Check if this is a known enum type (by FQN or simple name)
    if enum_values_map:
        values = enum_values_map.get(java_type) or enum_values_map.get(simple)
        if values:
            return {"type": "string", "enum": values}

    # Unknown domain types — treat as string
    return {"type": "string"}


def extract_xml_body(response_text):
    """Extract the XML body from an HttpResponse text field."""
    match = re.search(r"(<[^>]+>.*</[^>]+>)", response_text, re.DOTALL)
    if match:
        return match.group(1)
    return None


def schema_id_from_type(java_type):
    """Derive a schema component name from a fully-qualified Java type."""
    return java_type.split(".")[-1].replace(";", "").replace("[", "List")


# ---------------------------------------------------------------------------
# URL / parameter parsing
# ---------------------------------------------------------------------------

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
    path_part = url_text.split("datapathdnsnameofappliance")[-1]
    if "?" in path_part:
        pure_path, query_part = path_part.split("?", 1)
    elif "[?" in path_part:
        pure_path, query_part = path_part.split("[?", 1)
    else:
        pure_path = path_part
        query_part = ""

    pure_path = pure_path.split("[")[0]
    path = pure_path.replace("{unique identifier or attribute}", "{id}")
    if path.endswith("/") and len(path) > 1:
        path = path[:-1]
    if not path.startswith("/"):
        path = "/" + path

    fixed_params = []
    i = 0
    while i < len(query_part):
        if query_part[i] == "[":
            depth = 1
            i += 1
            while i < len(query_part) and depth > 0:
                if query_part[i] == "[":
                    depth += 1
                elif query_part[i] == "]":
                    depth -= 1
                i += 1
        else:
            start = i
            while i < len(query_part) and query_part[i] != "[":
                i += 1
            fixed_part = query_part[start:i]
            parts = [p for p in fixed_part.split("&") if p]
            for p in parts:
                p = re.sub(r"=\{[^}]+\}", "", p).strip()
                if p:
                    fixed_params.append(p)

    return path, fixed_params


# ---------------------------------------------------------------------------
# Main generation
# ---------------------------------------------------------------------------

REST_ACTION_MAP = {
    "CREATE": "post",
    "DELETE": "delete",
    "SHOW": "get",
    "LIST": "get",
    "MODIFY": "put",
    "BULK_MODIFY": "put",
}

# ---------------------------------------------------------------------------
# Request body definitions
# ---------------------------------------------------------------------------
# Maps handler simple name -> request body schema key
REQUEST_BODY_MAP = {
    "StageObjectsJobRequestHandler": "Objects",
    "CreateGetJobRequestHandler": "Objects",
    "CreatePutJobRequestHandler": "Objects",
    "CreateVerifyJobRequestHandler": "Objects",
    "EjectStorageDomainBlobsRequestHandler": "Objects",
    "GetPhysicalPlacementForObjectsRequestHandler": "Objects",
    "GetPhysicalPlacementForObjectsWithFullDetailsRequestHandler": "Objects",
    "VerifyPhysicalPlacementForObjectsRequestHandler": "Objects",
    "VerifyPhysicalPlacementForObjectsWithFullDetailsRequestHandler": "Objects",
    "CreateObjectRequestHandler": "string",
    "DeleteObjectsRequestHandler": "Delete",
    "GetBlobPersistenceRequestHandler": "BlobIds",
    "ReplicatePutJobRequestHandler": "JobToReplicate",
    "ClearSuspectBlobAzureTargetsRequestHandler": "Ids",
    "ClearSuspectBlobDs3TargetsRequestHandler": "Ids",
    "ClearSuspectBlobPoolsRequestHandler": "Ids",
    "ClearSuspectBlobS3TargetsRequestHandler": "Ids",
    "ClearSuspectBlobTapesRequestHandler": "Ids",
    "MarkSuspectBlobAzureTargetsAsDegradedRequestHandler": "Ids",
    "MarkSuspectBlobDs3TargetsAsDegradedRequestHandler": "Ids",
    "MarkSuspectBlobPoolsAsDegradedRequestHandler": "Ids",
    "MarkSuspectBlobS3TargetsAsDegradedRequestHandler": "Ids",
    "MarkSuspectBlobTapesAsDegradedRequestHandler": "Ids",
}

# Request body schemas — these are added to components/schemas
REQUEST_BODY_SCHEMAS = {
    "Objects": {
        "type": "object",
        "properties": {
            "Object": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "Name": {"type": "string"},
                        "Length": {"type": "integer", "format": "int64"},
                        "Offset": {"type": "integer", "format": "int64"},
                        "VersionId": {"type": "string"}
                    },
                    "required": ["Name"]
                }
            }
        },
        "required": ["Object"]
    },
    "Delete": {
        "type": "object",
        "properties": {
            "Quiet": {"type": "boolean"},
            "Object": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "Key": {"type": "string"},
                        "VersionId": {"type": "string"}
                    },
                    "required": ["Key"]
                }
            }
        },
        "required": ["Object"]
    },
    "BlobIds": {
        "type": "object",
        "properties": {
            "blobIds": {
                "type": "array",
                "items": {"type": "string"}
            },
            "jobId": {"type": "string"}
        },
        "required": ["blobIds", "jobId"]
    },
    "JobToReplicate": {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "name": {"type": "string"},
            "blobs": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "objectId": {"type": "string"},
                        "byteOffset": {"type": "integer", "format": "int64"},
                        "length": {"type": "integer", "format": "int64"},
                        "checksum": {"type": "string"},
                        "checksumType": {"type": "string"}
                    },
                    "required": ["id", "objectId", "byteOffset", "length"]
                }
            },
            "chunks": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "chunkNumber": {"type": "integer", "format": "int32"},
                        "originalChunkId": {"type": "string"},
                        "entries": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "id": {"type": "string"},
                                    "blobId": {"type": "string"},
                                    "chunkId": {"type": "string"},
                                    "jobId": {"type": "string"},
                                    "orderIndex": {"type": "integer", "format": "int32"}
                                },
                                "required": ["id", "blobId", "chunkId", "jobId", "orderIndex"]
                            }
                        }
                    },
                    "required": ["id", "chunkNumber", "entries"]
                }
            },
            "objects": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "bucketId": {"type": "string"},
                        "name": {"type": "string"},
                        "type": {"type": "string"},
                        "creationDate": {"type": "integer", "format": "int64"},
                        "latest": {"type": "boolean"}
                    },
                    "required": ["id", "bucketId", "name"]
                }
            }
        },
        "required": ["id", "name", "blobs", "chunks", "objects"]
    },
    "Ids": {
        "type": "object",
        "properties": {
            "id": {
                "type": "array",
                "items": {"type": "string"}
            }
        },
        "required": ["id"]
    }
}


def generate(xml_path, java_base_dirs, contract_xml_path=None):
    """Parse the XML and Java sources, return a complete OpenAPI spec dict."""
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Parse contract XML for parameter types
    handler_param_types = {}
    if contract_xml_path:
        print("  Parsing contract XML for parameter types ...")
        handler_param_types = parse_contract_param_types(contract_xml_path)
        print(f"  Found parameter types for {len(handler_param_types)} handlers")

    oas = {
        "openapi": "3.0.3",
        "info": {
            "title": "Spectra S3 API",
            "description": "Comprehensive API for Spectra S3 generated from request handlers.",
            "version": "1.0.0"
        },
        "servers": [
            {
                "url": "http://datapathdnsnameofappliance",
                "description": "Spectra S3 Server"
            }
        ],
        "paths": {},
        "components": {"schemas": {}}
    }

    # ------------------------------------------------------------------
    # Phase 1: Scan Java interfaces
    # ------------------------------------------------------------------
    print("  Scanning Java interfaces ...")
    java_registry = scan_java_interfaces(java_base_dirs)
    # Collect enum names and their values
    # enum_values_map: maps both simple name and FQN to list of enum constant names
    enum_names = set()
    enum_values_map = {}
    for base_dir in java_base_dirs:
        if not os.path.isdir(base_dir):
            continue
        for dirpath, _, filenames in os.walk(base_dir):
            for fname in filenames:
                if not fname.endswith(".java"):
                    continue
                fpath = os.path.join(dirpath, fname)
                with open(fpath) as f:
                    content = f.read()
                m = re.search(r"\bpublic\s+enum\s+(\w+)", content)
                if m:
                    enum_name = m.group(1)
                    enum_names.add(enum_name)
                    values = parse_enum_values(content)
                    if values:
                        enum_values_map[enum_name] = values
                        # Also map by FQN derived from package + class name
                        pkg_match = re.search(r"^\s*package\s+([\w.]+)\s*;", content, re.MULTILINE)
                        if pkg_match:
                            fqn = pkg_match.group(1) + "." + enum_name
                            enum_values_map[fqn] = values
    print(f"  Found {len(java_registry)} interfaces, {len(enum_names)} enums ({len(enum_values_map)//2} with parsed values)")

    # ------------------------------------------------------------------
    # Phase 2: Build paths and XML-inferred schemas
    # ------------------------------------------------------------------
    schemas = {}

    # Track which schema_ids map to which Java FQN simple names
    schema_to_java_name = {}

    for handler in root.findall("RequestHandler"):
        handler_name = handler.get("Name")
        doc_el = handler.find("Documentation")
        doc = doc_el.text if doc_el is not None else ""
        req_requirements = [r.text for r in handler.findall("RequestRequirements") if r.text]

        is_aws_style = any("Must be an AWS-style request" in r for r in req_requirements)
        has_bucket = any("Must include an S3 bucket specification" in r for r in req_requirements)
        has_object = any("Must include an S3 object specification" in r for r in req_requirements)

        # Determine path
        sample_url_tag = handler.find("SampleUrl")
        fixed_params = []
        if sample_url_tag is not None and sample_url_tag.text:
            path, fixed_params = parse_sample_url(sample_url_tag.text)
        elif is_aws_style:
            if has_bucket and has_object:
                path = "/{bucket}/{object}"
            elif has_bucket:
                path = "/{bucket}"
            else:
                path = "/"
        else:
            continue

        # Determine HTTP method
        method = "get"
        for req in req_requirements:
            if "Must be REST action" in req:
                action = req.split("Must be REST action ")[1]
                method = REST_ACTION_MAP.get(action, "get")
            elif "Must be HTTP request type" in req:
                method = req.split("Must be HTTP request type ")[1].lower()

        required_params, optional_params = parse_params(req_requirements)
        display_path = f"{path}?{'&'.join(sorted(fixed_params))}" if fixed_params else path

        if display_path not in oas["paths"]:
            oas["paths"][display_path] = {}

        # Build operation
        simple_name = handler_name.rsplit(".", 1)[-1]
        # Disambiguate duplicate simple names (e.g. amazons3 vs spectrads3 variants)
        classification = handler_name.split(".")[-2]  # e.g. "amazons3", "spectrads3"
        if classification == "amazons3":
            operation_id = f"S3{simple_name}"
        else:
            operation_id = simple_name
        operation = {
            "summary": simple_name,
            "description": doc,
            "operationId": operation_id,
            "tags": [classification],
            "parameters": [],
            "responses": {}
        }

        # Look up parameter types from contract XML (keyed by lowercase param name)
        # Values are (canonical_name, java_type) tuples
        param_types = handler_param_types.get(handler_name, {})

        def _param_lookup(param_name):
            """Get (canonical_name, schema) for a parameter, using contract types if available."""
            entry = param_types.get(param_name.lower())
            if entry:
                canonical_name, java_type = entry
                return canonical_name, contract_type_to_schema(java_type, enum_values_map)
            return param_name, {"type": "string"}

        # Path parameters
        for p in re.findall(r"\{([^}]+)\}", path):
            canonical, schema = _param_lookup(p)
            operation["parameters"].append({
                "name": canonical, "in": "path", "required": True,
                "schema": schema
            })

        # Required query parameters
        for p in required_params:
            canonical, schema = _param_lookup(p)
            if any(par["name"] == canonical for par in operation["parameters"]):
                continue
            default_val = next(
                (fp.split("=")[1] if "=" in fp else ""
                 for fp in fixed_params if fp == p or fp.startswith(p + "=")),
                None
            )
            if default_val is not None:
                schema["default"] = default_val
            param = {"name": canonical, "in": "query", "required": True, "schema": schema}
            operation["parameters"].append(param)

        # Optional query parameters
        for p in optional_params:
            canonical, schema = _param_lookup(p)
            if any(par["name"] == canonical for par in operation["parameters"]):
                continue
            operation["parameters"].append({
                "name": canonical, "in": "query", "required": False,
                "schema": schema
            })

        # Process SampleResponses — build responses AND accumulate schemas
        for sample_res in handler.findall("SampleResponses"):
            code = sample_res.find("HttpResponseCode").text or "200"
            res_type_el = sample_res.find("HttpResponseType")
            res_type_full = res_type_el.text if res_type_el is not None else None
            http_response_el = sample_res.find("HttpResponse")
            res_text = http_response_el.text if http_response_el is not None else ""

            if code not in operation["responses"]:
                operation["responses"][code] = {"description": f"Response for {code}"}

            if res_type_full and res_type_full != "null":
                sid = schema_id_from_type(res_type_full)

                # Remember the Java simple class name for this schema_id
                # Strip array prefix [L and trailing ; from JVM type descriptors
                # Handle inner classes: Outer$Inner -> just keep Inner for lookup,
                # but also keep Outer for fallback
                clean_type = res_type_full.lstrip("[L").rstrip(";")
                java_simple = clean_type.rsplit(".", 1)[-1]  # e.g. "Bucket" or "TapeFailuresResponseBuilder$TapeFailuresApiBean"
                schema_to_java_name[sid] = java_simple

                # Infer schema from XML body
                xml_body = extract_xml_body(res_text)
                if xml_body:
                    try:
                        sample_root = ET.fromstring(xml_body)
                        # Skip empty wrapper elements like <Data></Data>
                        # which produce a misleading {"type": "string"} schema
                        if len(sample_root) > 0 or sample_root.text:
                            inferred = infer_schema_from_element(sample_root)
                            inferred["xml"] = {"name": sample_root.tag}
                            schemas[sid] = merge_schemas(schemas.get(sid), inferred)
                    except ET.ParseError:
                        pass

                operation["responses"][code]["content"] = {
                    "application/json": {
                        "schema": {"$ref": f"#/components/schemas/{sid}"}
                    }
                }

                # Fallback stub if no XML was parsed for this type at all
                if sid not in schemas:
                    schemas[sid] = {"type": "object"}

        # Add request body if this handler has one
        handler_simple = handler_name.rsplit(".", 1)[-1]
        body_schema_key = REQUEST_BODY_MAP.get(handler_simple)
        if body_schema_key:
            if body_schema_key == "string":
                operation["requestBody"] = {
                    "required": True,
                    "content": {
                        "application/octet-stream": {
                            "schema": {"type": "string", "format": "binary"}
                        }
                    }
                }
            else:
                # Determine content type: JSON for BlobIds/JobToReplicate, XML for others
                if body_schema_key in ("BlobIds", "JobToReplicate"):
                    content_type = "application/json"
                else:
                    content_type = "application/json"
                operation["requestBody"] = {
                    "required": True,
                    "content": {
                        content_type: {
                            "schema": {"$ref": f"#/components/schemas/{body_schema_key}"}
                        }
                    }
                }

        # Prefer spectrads3 handler when there's a path+method collision
        if method in oas["paths"][display_path]:
            if "spectrads3" in handler_name:
                oas["paths"][display_path][method] = operation
        else:
            oas["paths"][display_path][method] = operation

    # Ensure every operation has at least one response with content.
    # The Kotlin client generator requires this.
    empty_response_count = 0
    for path_item in oas["paths"].values():
        for op in path_item.values():
            responses = op.get("responses", {})
            has_content = any("content" in r for r in responses.values())
            if not has_content:
                # Add a 204 response with empty content to satisfy the generator
                responses.setdefault("204", {})
                responses["204"]["description"] = "No Content"
                responses["204"]["content"] = {
                    "application/json": {
                        "schema": {"type": "object"}
                    }
                }
                empty_response_count += 1
    if empty_response_count:
        print(f"  Added default response content to {empty_response_count} operations")

    # Add request body schemas to components
    for schema_key, schema_def in REQUEST_BODY_SCHEMAS.items():
        if schema_key not in schemas:
            schemas[schema_key] = schema_def

    # ------------------------------------------------------------------
    # Phase 3: Enrich schemas with Java interface definitions
    # ------------------------------------------------------------------
    print("  Enriching schemas from Java interfaces ...")
    enriched_count = 0
    filled_count = 0

    for sid, java_name in schema_to_java_name.items():
        # Try to find the Java interface for this schema
        # For inner classes like "TapeFailuresResponseBuilder$TapeFailuresApiBean",
        # try the inner name "TapeFailuresApiBean" and also the outer
        candidates = [java_name]
        if "$" in java_name:
            candidates.append(java_name.split("$")[-1])
            candidates.append(java_name.split("$")[0])

        java_schema = None
        for candidate in candidates:
            if candidate in java_registry:
                java_schema = build_java_schema(candidate, java_registry, enum_names, enum_values_map)
                if java_schema:
                    break

        if not java_schema:
            continue

        existing = schemas.get(sid)
        has_properties = existing and "properties" in existing
        if not has_properties:
            # No usable XML-inferred schema — use Java schema as the base
            schemas[sid] = java_schema
            filled_count += 1
        else:
            # Merge Java schema into existing XML-inferred schema.
            # XML schema is primary (has real runtime type info),
            # Java adds missing fields.
            schemas[sid] = merge_schemas(existing, java_schema)
            enriched_count += 1

    print(f"  Java enrichment: {filled_count} schemas filled from Java, "
          f"{enriched_count} existing schemas enriched with additional fields")

    # ------------------------------------------------------------------
    # Phase 4: Handle special-case schemas
    # ------------------------------------------------------------------
    # java.lang.String responses return raw JSON, not XML-marshaled objects.
    # Model as a plain string type rather than an empty object stub.
    if "String" in schemas and "properties" not in schemas["String"]:
        schemas["String"] = {"type": "string"}

    # ------------------------------------------------------------------
    # Phase 5: Fix enum properties in XML-inferred schemas
    # ------------------------------------------------------------------
    # XML inference sees enum values as plain strings. Use the Java interface
    # field definitions to identify which properties are enums and add the
    # enum values to the schema.
    print("  Fixing enum properties in schemas ...")
    enum_fixes = 0
    for sid, java_name in schema_to_java_name.items():
        schema = schemas.get(sid)
        if not schema or "properties" not in schema:
            continue

        # Find the Java interface for this schema
        candidates = [java_name]
        if "$" in java_name:
            candidates.append(java_name.split("$")[-1])
            candidates.append(java_name.split("$")[0])

        # Resolve all fields from the interface chain
        all_fields = []
        for candidate in candidates:
            if candidate in java_registry:
                all_fields = resolve_all_fields(candidate, java_registry)
                if all_fields:
                    break

        if not all_fields:
            continue

        # Build a map: field_name -> java_return_type
        field_type_map = {}
        for field_name, java_type, _ in all_fields:
            if field_name not in field_type_map:
                field_type_map[field_name] = java_type

        # Fix properties that are plain {"type": "string"} but should be enums
        for prop_name, prop_def in schema["properties"].items():
            if prop_def.get("type") == "string" and "enum" not in prop_def:
                java_type = field_type_map.get(prop_name)
                if java_type and java_type in enum_values_map:
                    prop_def["enum"] = enum_values_map[java_type]
                    enum_fixes += 1

    print(f"  Fixed {enum_fixes} enum properties in schemas")

    # ------------------------------------------------------------------
    # Phase 6: Strip internal "xml" metadata from all schemas
    # ------------------------------------------------------------------
    def strip_xml_metadata(obj):
        """Recursively remove 'xml' keys from a schema dict."""
        if isinstance(obj, dict):
            obj.pop("xml", None)
            for v in obj.values():
                strip_xml_metadata(v)
        elif isinstance(obj, list):
            for item in obj:
                strip_xml_metadata(item)

    for schema in schemas.values():
        strip_xml_metadata(schema)

    oas["components"]["schemas"] = schemas
    return oas


# ---------------------------------------------------------------------------
# Validation — compare against existing swagger.json
# ---------------------------------------------------------------------------

def compare_with_existing(new_spec, existing_path):
    """Compare the newly generated spec against the existing swagger.json."""
    try:
        with open(existing_path) as f:
            existing = json.load(f)
    except FileNotFoundError:
        print(f"  [skip] {existing_path} not found, nothing to compare against")
        return

    # Paths
    new_paths = set(new_spec["paths"].keys())
    old_paths = set(existing["paths"].keys())
    added_paths = new_paths - old_paths
    removed_paths = old_paths - new_paths
    common_paths = new_paths & old_paths

    print(f"\n--- Comparison with {existing_path} ---")
    print(f"  Paths:   {len(new_paths)} new, {len(old_paths)} existing, {len(common_paths)} in common")
    if added_paths:
        print(f"  Added paths ({len(added_paths)}):")
        for p in sorted(added_paths):
            print(f"    + {p}")
    if removed_paths:
        print(f"  Removed paths ({len(removed_paths)}):")
        for p in sorted(removed_paths):
            print(f"    - {p}")

    # Operations per common path
    ops_match = 0
    ops_diff = 0
    for p in common_paths:
        new_methods = set(new_spec["paths"][p].keys())
        old_methods = set(existing["paths"][p].keys())
        if new_methods == old_methods:
            ops_match += 1
        else:
            ops_diff += 1
            if ops_diff <= 5:
                print(f"  Method diff at {p}: new={sorted(new_methods)} existing={sorted(old_methods)}")
    if ops_diff > 5:
        print(f"  ... and {ops_diff - 5} more method differences")
    print(f"  Operations: {ops_match} paths match methods, {ops_diff} differ")

    # Schemas
    new_schemas = set(new_spec["components"]["schemas"].keys())
    old_schemas = set(existing["components"]["schemas"].keys())
    added_schemas = new_schemas - old_schemas
    removed_schemas = old_schemas - new_schemas
    common_schemas = new_schemas & old_schemas

    print(f"  Schemas: {len(new_schemas)} new, {len(old_schemas)} existing, {len(common_schemas)} in common")
    if added_schemas:
        print(f"  Added schemas ({len(added_schemas)}):")
        for s in sorted(added_schemas):
            print(f"    + {s}")
    if removed_schemas:
        print(f"  Removed schemas ({len(removed_schemas)}):")
        for s in sorted(removed_schemas):
            print(f"    - {s}")

    # Schema property coverage
    new_with_props = sum(1 for s in new_spec["components"]["schemas"].values() if "properties" in s)
    old_with_props = sum(1 for s in existing["components"]["schemas"].values() if "properties" in s)
    print(f"  Schemas with properties: {new_with_props}/{len(new_schemas)} new vs {old_with_props}/{len(old_schemas)} existing")

    # Spot-check common schemas for property differences
    diffs_shown = 0
    for sid in sorted(common_schemas):
        new_props = set(new_spec["components"]["schemas"][sid].get("properties", {}).keys())
        old_props = set(existing["components"]["schemas"][sid].get("properties", {}).keys())
        if new_props != old_props:
            added_p = new_props - old_props
            removed_p = old_props - new_props
            if diffs_shown < 10:
                parts = []
                if added_p:
                    parts.append(f"+{sorted(added_p)}")
                if removed_p:
                    parts.append(f"-{sorted(removed_p)}")
                print(f"  Schema '{sid}' property diff: {', '.join(parts)}")
            diffs_shown += 1
    if diffs_shown > 10:
        print(f"  ... and {diffs_shown - 10} more schema property differences")
    if diffs_shown == 0:
        print("  All common schemas have identical property sets")

    print("--- End comparison ---\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    xml_path = sys.argv[1] if len(sys.argv) > 1 else "server/src/main/resources/request-handlers-full.xml"
    output_path = "xm-api-swagger.json"
    existing_path = "swagger.json"

    # Java source directories to scan for interfaces
    base = os.path.dirname(os.path.abspath(__file__))
    java_dirs = [
        os.path.join(base, "common", "src", "main", "java"),
        os.path.join(base, "server", "src", "main", "java"),
        os.path.join(base, "util", "src", "main", "java"),
    ]

    contract_xml_path = os.path.join(base, "server", "src", "main", "resources", "request-handlers-contract.xml")

    print(f"Parsing {xml_path} ...")
    spec = generate(xml_path, java_dirs, contract_xml_path)

    total = len(spec["components"]["schemas"])
    with_props = sum(1 for s in spec["components"]["schemas"].values() if "properties" in s)
    empty = [n for n, s in spec["components"]["schemas"].items() if "properties" not in s]
    print(f"Generated {len(spec['paths'])} paths, {total} schemas ({with_props} with properties)")
    if empty:
        print(f"  Schemas still without properties: {empty}")

    with open(output_path, "w") as f:
        json.dump(spec, f, indent=2)
    print(f"Written to {output_path}")

    compare_with_existing(spec, existing_path)


if __name__ == "__main__":
    main()
