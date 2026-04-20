#!/usr/bin/env python3
"""
Convert Swagger 2.0 spec to OpenAPI 3.1.0 spec.
Fixes all identified issues:
1. swagger/openapi version field
2. host/basePath/schemes -> servers array
3. definitions -> components/schemas
4. Inline param types -> schema: wrapper
5. body params -> requestBody
6. Response $refs from #/definitions/ -> #/components/schemas/
7. Paths with query strings in URL (move to query params)
8. Duplicate operationIds (disambiguate)
9. Definitions with dots in names (sanitize)
10. summaries as class names -> human-readable descriptions
"""

import json
import re

INPUT_FILE = '/Users/ashapillai/devRepo/redline/product/frontend/swagger.json'
OUTPUT_FILE = '/Users/ashapillai/devRepo/redline/product/frontend/openapi.json'


def sanitize_ref_name(name):
    """Replace dots in definition names with underscores for valid JSON ref names."""
    return name.replace('.', '_')


def convert_type_to_schema(param):
    """Convert Swagger 2.0 inline param type to OAS3 schema wrapper."""
    schema = {}

    if 'type' in param:
        t = param['type']
        if t == 'integer':
            schema['type'] = 'integer'
            if 'format' in param:
                schema['format'] = param['format']
        elif t == 'number':
            schema['type'] = 'number'
            if 'format' in param:
                schema['format'] = param['format']
        elif t == 'boolean':
            schema['type'] = 'boolean'
        elif t == 'array':
            schema['type'] = 'array'
            if 'items' in param:
                schema['items'] = convert_schema_refs(param['items'])
        else:
            schema['type'] = 'string'

        if 'enum' in param:
            schema['enum'] = param['enum']
        if 'default' in param:
            schema['default'] = param['default']
        if 'minimum' in param:
            schema['minimum'] = param['minimum']
        if 'maximum' in param:
            schema['maximum'] = param['maximum']
    elif '$ref' in param:
        schema['$ref'] = convert_ref(param['$ref'])

    return schema


def convert_ref(ref):
    """Convert #/definitions/X to #/components/schemas/X, sanitizing name."""
    if ref.startswith('#/definitions/'):
        name = ref[len('#/definitions/'):]
        sanitized = sanitize_ref_name(name)
        return f'#/components/schemas/{sanitized}'
    return ref


def convert_schema_refs(schema):
    """Recursively convert $ref in a schema from definitions to components/schemas."""
    if not isinstance(schema, dict):
        return schema

    result = {}
    for k, v in schema.items():
        if k == '$ref':
            result[k] = convert_ref(v)
        elif k == 'properties':
            result[k] = {pk: convert_schema_refs(pv) for pk, pv in v.items()}
        elif k in ('items', 'additionalProperties', 'not'):
            result[k] = convert_schema_refs(v)
        elif k in ('allOf', 'anyOf', 'oneOf'):
            result[k] = [convert_schema_refs(s) for s in v]
        elif isinstance(v, dict):
            result[k] = convert_schema_refs(v)
        elif isinstance(v, list):
            result[k] = [convert_schema_refs(item) if isinstance(item, dict) else item for item in v]
        else:
            result[k] = v
    return result


def convert_response_schema(resp):
    """Convert a Swagger 2.0 response to OAS3 response."""
    if not isinstance(resp, dict):
        return resp

    result = {'description': resp.get('description', 'Response')}

    if 'schema' in resp:
        schema = resp['schema']
        converted_schema = convert_schema_refs(schema)
        result['content'] = {
            'application/xml': {
                'schema': converted_schema
            }
        }

    if 'headers' in resp:
        result['headers'] = resp['headers']

    return result


def extract_query_param_from_path(path):
    """Extract the path and operation query param from paths like '/_rest_/foo/?operation=BAR'."""
    if '?' in path:
        base, query = path.split('?', 1)
        # Remove trailing slash if it was before the ?
        clean_path = base  # keep trailing slash if it was there
        # parse query params
        params = {}
        for part in query.split('&'):
            if '=' in part:
                k, v = part.split('=', 1)
                params[k] = v
            else:
                params[part] = None
        return clean_path, params
    return path, {}


def make_operation_id(op_id, method, path):
    """Generate a unique operationId based on the handler name, method and path."""
    # Convert from PascalCase to camelCase
    if op_id:
        # Make it camelCase
        camel = op_id[0].lower() + op_id[1:] if op_id else ''
        return camel
    # Fallback: generate from method + path
    path_clean = re.sub(r'[^a-zA-Z0-9]', '_', path).strip('_')
    return f'{method}_{path_clean}'


def make_human_summary(op_id, description):
    """Generate a human-readable summary from the operationId and description."""
    if not op_id:
        return description[:80] if description else 'Operation'

    # Split on camelCase/PascalCase boundaries
    # e.g. CreateBucketRequestHandler -> Create Bucket
    name = op_id.replace('RequestHandler', '').replace('Handler', '')
    # Split PascalCase
    words = re.sub(r'([A-Z])', r' \1', name).strip().split()
    # Remove duplicated spaces and trailing words like 'Request'
    if words and words[-1] == 'Request':
        words = words[:-1]

    return ' '.join(words) if words else op_id


def convert_parameter(param, path_query_params=None):
    """Convert a single Swagger 2.0 parameter to OAS3."""
    if param.get('in') == 'body':
        # Body params are handled separately (converted to requestBody)
        return None

    result = {
        'name': param['name'],
        'in': param.get('in', 'query'),
        'required': param.get('required', False),
    }

    if 'description' in param:
        result['description'] = param['description']

    # Build schema from type info
    schema = {}
    if 'schema' in param:
        schema = convert_schema_refs(param['schema'])
    elif 'type' in param:
        t = param['type']
        if t == 'integer':
            schema = {'type': 'integer'}
            if 'format' in param:
                schema['format'] = param['format']
        elif t == 'number':
            schema = {'type': 'number'}
            if 'format' in param:
                schema['format'] = param['format']
        elif t == 'boolean':
            schema = {'type': 'boolean'}
        elif t == 'array':
            schema = {'type': 'array'}
            if 'items' in param:
                schema['items'] = convert_schema_refs(param['items'])
        else:
            schema = {'type': 'string'}

        if 'enum' in param:
            schema['enum'] = param['enum']
        if 'default' in param:
            schema['default'] = param['default']
    elif '$ref' in param:
        schema = {'$ref': convert_ref(param['$ref'])}
    else:
        schema = {'type': 'string'}

    result['schema'] = schema
    return result


def convert_path(path):
    """Convert path template to OAS3 format (remove trailing slash issues, keep path params)."""
    # OAS3 allows trailing slashes, keep them as-is
    return path


def build_request_body(params, method):
    """Build an OAS3 requestBody from body parameters."""
    body_params = [p for p in params if p.get('in') == 'body']
    if not body_params:
        return None

    # Use the first body param (there should only be one)
    body_param = body_params[0]
    schema = {}
    if 'schema' in body_param:
        schema = convert_schema_refs(body_param['schema'])

    return {
        'required': body_param.get('required', True),
        'content': {
            'application/xml': {
                'schema': schema
            }
        }
    }


def fix_misplaced_base_ops(paths_data):
    """Fix cases where swagger placed a specific-action handler at the base path instead
    of the general Modify handler.

    Pattern detected: base path (no ?) has an operation with required non-path query
    params AND a ?handler=ModifyXxx variant exists for the same path+method.

    Fix:
      - Promote the Modify* handler to the base path.
      - If the original base-path op is already covered by an existing ?operation=X
        variant (same operationId), discard it (it's a duplicate).
      - Otherwise, keep it as a ?handler=<operationId> variant so it still gets a path.
      - Remove the now-promoted ?handler=Modify* entry from paths_data.
    """
    import copy
    paths = copy.deepcopy(paths_data)

    # Index all ?handler=X variants: (base_path, method) -> {handlerName: (full_path, op)}
    handler_variants = {}
    for path, item in paths.items():
        if '?handler=' not in path:
            continue
        base = path.split('?')[0]
        handler = path.split('?handler=')[1].split('&')[0]
        for method, op in item.items():
            if not isinstance(op, dict):
                continue
            key = (base, method)
            handler_variants.setdefault(key, {})[handler] = (path, op)

    # Find base paths where promotion is needed
    promotions = []
    for path, item in paths.items():
        if '?' in path:
            continue
        for method, op in item.items():
            if not isinstance(op, dict):
                continue
            required_query = [p['name'] for p in op.get('parameters', [])
                              if p.get('in') == 'query' and p.get('required')]
            if not required_query:
                continue
            key = (path, method)
            modify_handlers = [(h, info) for h, info in handler_variants.get(key, {}).items()
                               if 'Modify' in h]
            if modify_handlers:
                handler_name, (handler_path, modify_op) = modify_handlers[0]
                promotions.append((path, method, op, handler_name, handler_path, modify_op))

    # Apply promotions
    for base_path, method, old_base_op, handler_name, handler_path, modify_op in promotions:
        op_id = old_base_op.get('operationId', '')

        # Check if there's already a ?operation=X variant covering the same operationId
        has_operation_variant = any(
            op2.get('operationId') == op_id
            for p, item in paths.items()
            if '?operation=' in p and p.startswith(base_path)
            for m, op2 in item.items()
            if isinstance(op2, dict)
        )

        # Promote Modify* to base path
        paths[base_path][method] = modify_op

        # Remove the now-promoted handler variant entry
        del paths[handler_path][method]
        if not any(isinstance(v, dict) for v in paths[handler_path].values()):
            del paths[handler_path]

        # Keep the demoted op only if not already covered by an operation variant
        if not has_operation_variant:
            demoted_path = f'{base_path}?handler={op_id}'
            paths.setdefault(demoted_path, {})[method] = old_base_op

    return paths


def merge_operation_variants(paths_data):
    """Merge all ?operation=X swagger variants for the same base_path+method into a
    single OAS3 operation at the real base path.

    After fix_misplaced_base_ops each base path (no ?) may already have a Modify*
    operation.  All ?operation=X variants for that base+method are folded into that
    existing operation (or a new one when none exists).

    Result:
      - 'operation' becomes an optional query param (enum of all values) when a base
        op already exists (Modify is the implicit default when operation is absent).
      - 'operation' becomes a required query param when no base op existed.
      - requestBody is preserved if ANY variant carried one.
      - All extra query params from variants are unioned and made optional.
      - All responses are unioned.
      - The ?operation=X paths are removed from paths_data.
    """
    import copy
    paths = copy.deepcopy(paths_data)

    # Collect all ?operation=X variants grouped by (base_path, method)
    variants_by_base = {}
    for path in list(paths.keys()):
        if 'operation=' not in path:
            continue
        base = path.split('?')[0]
        query_str = path.split('?', 1)[1]
        qparams = {}
        for part in query_str.split('&'):
            if '=' in part:
                k, v = part.split('=', 1)
                qparams[k] = v
        if 'operation' not in qparams:
            continue
        for method, op in paths[path].items():
            if not isinstance(op, dict):
                continue
            key = (base, method)
            variants_by_base.setdefault(key, []).append((path, qparams, copy.deepcopy(op)))

    # Merge each group into its base path
    for (base_path, method), variants in variants_by_base.items():
        variants = sorted(variants, key=lambda x: x[0])  # deterministic order

        existing_base_op = None
        if base_path in paths and isinstance(paths[base_path].get(method), dict):
            existing_base_op = copy.deepcopy(paths[base_path][method])

        # operation is optional only when the base op is a Modify-style handler
        # (i.e. it has no required Operation query param of its own).
        # When the base op itself required an Operation param it is a specific-action
        # handler displaced to the base path, so operation stays required.
        base_op_requires_operation = False
        if existing_base_op:
            for p in existing_base_op.get('parameters', []):
                if p.get('name', '').lower() == 'operation' and p.get('in') == 'query' and p.get('required'):
                    base_op_requires_operation = True
                    break
        operation_required = existing_base_op is None or base_op_requires_operation

        # Collect data from all variants
        all_op_values = []
        body_param = None
        extra_params = {}   # param_name_lower -> swagger param dict
        all_responses = {}

        for full_path, qparams, op in variants:
            op_val = qparams.get('operation', '').lower()
            if op_val and op_val not in all_op_values:
                all_op_values.append(op_val)

            for param in op.get('parameters', []):
                if param.get('in') == 'body':
                    if body_param is None:
                        body_param = copy.deepcopy(param)
                    continue
                pname = param.get('name', '').lower()
                if pname == 'operation':
                    continue
                if pname not in extra_params:
                    extra_params[pname] = copy.deepcopy(param)

            for code, resp in op.get('responses', {}).items():
                if code not in all_responses:
                    all_responses[code] = copy.deepcopy(resp)

        # Base op: either promoted Modify* or first variant
        if existing_base_op:
            merged_op = existing_base_op
            # If the base op was a displaced specific-action handler (not a Modify-style
            # default), rename it to something generic so the merged operation's name
            # isn't misleadingly tied to one specific action.
            if base_op_requires_operation:
                base_op_id = existing_base_op.get('operationId', '')
                if 'Modify' not in base_op_id:
                    # Derive a generic name from the path
                    path_clean = (base_path.strip('/')
                                  .replace('_rest_/', '')
                                  .replace('/', '_')
                                  .replace('{', '').replace('}', ''))
                    parts = [p for p in path_clean.split('_') if p]
                    camel = parts[0] + ''.join(p.capitalize() for p in parts[1:])
                    merged_op['operationId'] = camel + 'Operations'
                    merged_op['summary'] = re.sub(r'([A-Z])', r' \1',
                        path_clean.replace('_', ' ').title().replace(' ', '')).strip()
        else:
            _, _, first_op = variants[0]
            merged_op = copy.deepcopy(first_op)
            # Strip body from parameters; it will be re-added below
            merged_op['parameters'] = [p for p in merged_op.get('parameters', [])
                                        if p.get('in') != 'body']
            # Derive a generic operationId and summary from the path
            path_clean = (base_path.strip('/')
                          .replace('_rest_/', '')
                          .replace('/', '_')
                          .replace('{', '').replace('}', ''))
            parts = [p for p in path_clean.split('_') if p]
            camel = parts[0] + ''.join(p.capitalize() for p in parts[1:]) if parts else 'unknown'
            merged_op['operationId'] = camel + 'Operations'

        # Rebuild parameters: path params + operation enum + base query params + extra
        existing_params = merged_op.get('parameters', [])
        path_params = [p for p in existing_params if p.get('in') == 'path']
        base_query = [p for p in existing_params
                      if p.get('in') == 'query' and p.get('name', '').lower() != 'operation']
        base_query_names = {p.get('name', '').lower() for p in base_query}

        op_param = {
            'name': 'operation',
            'in': 'query',
            'required': operation_required,
            'schema': {'type': 'string', 'enum': all_op_values},
            'description': ('Operation to perform' if operation_required
                            else 'Specific operation (absent = modify)'),
        }

        # All existing param names (path + query) — avoid adding duplicates
        existing_names = {p.get('name', '').lower() for p in path_params + base_query}

        extra_list = []
        for pname, param in extra_params.items():
            if pname not in base_query_names and pname not in existing_names:
                p_copy = copy.deepcopy(param)
                p_copy['required'] = False
                extra_list.append(p_copy)

        new_params = path_params + [op_param] + base_query + extra_list
        if body_param:
            # Mark body as not strictly required (only needed for some operations)
            body_copy = copy.deepcopy(body_param)
            body_copy['required'] = False
            new_params.append(body_copy)
        merged_op['parameters'] = new_params

        # Merge responses
        merged_responses = dict(merged_op.get('responses', {}))
        for code, resp in all_responses.items():
            if code not in merged_responses:
                merged_responses[code] = resp
        merged_op['responses'] = merged_responses

        # Store merged op at base path
        if base_path not in paths:
            paths[base_path] = {}
        paths[base_path][method] = merged_op

        # Remove all consumed variant paths
        for full_path, _, _ in variants:
            if method in paths.get(full_path, {}):
                del paths[full_path][method]
            if full_path in paths and not any(isinstance(v, dict) for v in paths[full_path].values()):
                del paths[full_path]

    return paths


def convert_swagger_to_oas3(swagger_data):
    """Main conversion function."""
    oas3 = {}

    # 1. openapi version
    oas3['openapi'] = '3.1.0'

    # 2. info section
    oas3['info'] = {
        'title': swagger_data.get('info', {}).get('title', 'Spectra Logic S3 Server API'),
        'description': swagger_data.get('info', {}).get('description', 'BlackPearl DS3 API'),
        'version': swagger_data.get('info', {}).get('version', '1.0.0'),
        'contact': {
            'name': 'Spectra Logic',
            'url': 'https://www.spectralogic.com'
        }
    }

    # 3. servers array (replacing host/basePath/schemes)
    host = swagger_data.get('host', 'datapathdnsnameofappliance')
    basePath = swagger_data.get('basePath', '/')
    schemes = swagger_data.get('schemes', ['http', 'https'])

    servers = []
    for scheme in schemes:
        url = f'{scheme}://{host}'
        if basePath and basePath != '/':
            url += basePath
        servers.append({
            'url': url,
            'description': f'BlackPearl appliance ({scheme.upper()})'
        })

    oas3['servers'] = servers

    # 4. Security schemes
    oas3['components'] = {}
    oas3['components']['securitySchemes'] = {
        'basicAuth': {
            'type': 'http',
            'scheme': 'basic',
            'description': 'HTTP Basic Authentication with AWS S3 compatible credentials'
        },
        'awsS3Auth': {
            'type': 'apiKey',
            'in': 'header',
            'name': 'Authorization',
            'description': 'AWS S3 compatible HMAC authentication signature'
        }
    }

    # 5. Convert definitions to components/schemas
    definitions = swagger_data.get('definitions', {})
    schemas = {}
    for def_name, def_schema in definitions.items():
        sanitized_name = sanitize_ref_name(def_name)
        schemas[sanitized_name] = convert_schema_refs(def_schema)

    oas3['components']['schemas'] = schemas

    # Add a common error response schema reference
    oas3['components']['responses'] = {
        'ErrorResponse': {
            'description': 'Error response',
            'content': {
                'application/xml': {
                    'schema': {'$ref': '#/components/schemas/HttpErrorResultApiBean'}
                }
            }
        }
    }

    # 6. Convert paths
    # Pre-process: fix misplaced base-path operations (promote Modify* handlers)
    paths_data = fix_misplaced_base_ops(swagger_data.get('paths', {}))

    # Track operationIds to ensure uniqueness
    op_id_tracker = {}

    oas3_paths = {}

    def get_variant_path(base_path, query_params, operation_id):
        """Derive the OAS3 path key for a swagger path that has query params.

        For ?operation=X paths: embed operation value in the path key as a real
        query string (e.g. /_rest_/bucket/{id}?operation=start_bulk_get).  This
        keeps the real base URL visible and gives each handler a unique key.

        For ?handler=X only paths: fall back to a synthetic sub-path segment
        because handler= is an internal dispatch param with no standard URL form.
        """
        operation_val = query_params.get('operation', '')
        handler_val = query_params.get('handler', '')

        if operation_val:
            op_lower = operation_val.lower()
            base = base_path.rstrip('/')
            if handler_val:
                # Dual-key: operation + handler — include both to keep the key unique
                handler_name = handler_val.replace('RequestHandler', '').replace('Handler', '')
                h_seg = re.sub(r'(?<=[a-z0-9])([A-Z])', r'_\1', handler_name).lower()
                return f'{base}?operation={op_lower}&handler={h_seg}'
            return f'{base}?operation={op_lower}'
        elif handler_val:
            # Handler-only: synthetic sub-path
            handler_name = handler_val.replace('RequestHandler', '').replace('Handler', '')
            segment = re.sub(r'(?<=[a-z0-9])([A-Z])', r'_\1', handler_name).lower()
            return f'{base_path.rstrip("/")}/{segment}/'
        else:
            name = operation_id.replace('RequestHandler', '').replace('Handler', '')
            segment = re.sub(r'(?<=[a-z0-9])([A-Z])', r'_\1', name).lower().strip('_')
            return f'{base_path.rstrip("/")}/{segment}/'

    # Single pass: each swagger path becomes its own OAS3 path.
    # Paths without '?' map directly; paths with '?' get a synthetic clean path
    # derived from the operation/handler value in the query string.
    for swagger_path in sorted(paths_data.keys()):
        path_item = paths_data[swagger_path]

        if '?' in swagger_path:
            base_path, url_query_params = extract_query_param_from_path(swagger_path)
        else:
            base_path = swagger_path
            url_query_params = {}

        for method, op in path_item.items():
            if not isinstance(op, dict):
                continue

            if url_query_params:
                operation_id = op.get('operationId', '')
                oas3_path = get_variant_path(base_path, url_query_params, operation_id)
            else:
                oas3_path = swagger_path

            # Resolve rare path+method conflicts (two swagger paths map to same OAS3 path)
            candidate = oas3_path
            counter = 2
            while candidate in oas3_paths and method in oas3_paths[candidate]:
                candidate = f'{oas3_path.rstrip("/")}_v{counter}/'
                counter += 1
            oas3_path = candidate

            # url_query_params carries the enum constraints for the operation param
            # (e.g. operation=START_BULK_PUT tells us Operation must equal that value)
            oas3_op = convert_operation(op, method, oas3_path, url_query_params, op_id_tracker)

            if oas3_path not in oas3_paths:
                oas3_paths[oas3_path] = {}
            oas3_paths[oas3_path][method] = oas3_op

    oas3['paths'] = oas3_paths

    # 7. Apply XML annotations to schemas whose actual wire format differs from the
    #    JSON-style property names the swagger definitions use.
    #
    #    JobToCreateApiBean: wire format is <Objects><Object Name="..." Size="..."/></Objects>
    #      - root element: Objects  (not JobToCreateApiBean)
    #      - items element: Object  (not S3ObjectToJobApiBean)
    #      - all item fields are XML attributes, not child elements
    schemas = oas3['components']['schemas']

    if 'JobToCreateApiBean' in schemas:
        schemas['JobToCreateApiBean'] = {
            'type': 'object',
            'xml': {'name': 'Objects'},
            'properties': {
                'Object': {
                    'type': 'array',
                    'xml': {'name': 'Object', 'wrapped': False},
                    'items': {'$ref': '#/components/schemas/S3ObjectToJobApiBean'}
                }
            }
        }

    if 'S3ObjectToJobApiBean' in schemas:
        schemas['S3ObjectToJobApiBean'] = {
            'type': 'object',
            'xml': {'name': 'Object'},
            'properties': {
                'Name':      {'type': 'string',  'xml': {'attribute': True}},
                'Size':      {'type': 'integer', 'format': 'int64', 'xml': {'attribute': True}},
                'Length':    {'type': 'integer', 'format': 'int64', 'xml': {'attribute': True}},
                'Offset':    {'type': 'integer', 'format': 'int64', 'xml': {'attribute': True}},
                'VersionId': {'type': 'string',  'xml': {'attribute': True}}
            }
        }

    # 9. Tags
    oas3['tags'] = [
        {
            'name': 'amazons3',
            'description': 'Amazon S3 compatible endpoints (AWS API)'
        },
        {
            'name': 'spectrads3',
            'description': 'Spectra Logic DS3 management endpoints'
        }
    ]

    return oas3


def convert_operation(op, method, path, extra_query_params, op_id_tracker):
    """Convert a single Swagger 2.0 operation to OAS3."""
    original_op_id = op.get('operationId', '')

    # Generate camelCase operationId
    camel_id = make_operation_id(original_op_id, method, path)

    # Make it unique
    if camel_id in op_id_tracker:
        op_id_tracker[camel_id] += 1
        # Distinguish by path context
        path_suffix = re.sub(r'[^a-zA-Z0-9]', '', path.replace('_rest_', '').replace('{id}', 'ById'))
        camel_id = f'{camel_id}_{path_suffix}' if path_suffix else f'{camel_id}_{op_id_tracker[camel_id]}'
        # Final dedup
        if camel_id in op_id_tracker:
            op_id_tracker[camel_id] = op_id_tracker.get(camel_id, 0) + 1
            camel_id = f'{camel_id}_{op_id_tracker[camel_id]}'
    else:
        op_id_tracker[camel_id] = 1

    description = op.get('description', '')
    summary = make_human_summary(original_op_id, description)

    oas3_op = {
        'operationId': camel_id,
        'summary': summary,
        'tags': op.get('tags', []),
    }

    if description:
        oas3_op['description'] = description

    # Convert parameters
    params = op.get('parameters', [])
    oas3_params = []

    # Build a case-insensitive lookup of URL query param names so we can add enum
    # constraints to existing swagger params instead of duplicating them.
    # Skip 'handler' — it is an internal dispatch param, not a client query param.
    url_enum_constraints = {
        qk.lower(): qv
        for qk, qv in extra_query_params.items()
        if qv is not None and qk.lower() != 'handler'
    }

    body_param = None
    for param in params:
        if param.get('in') == 'body':
            body_param = param
            continue
        converted = convert_parameter(param)
        if converted:
            # If this param matches a URL query param, pin its enum to that value
            param_name_lower = converted.get('name', '').lower()
            if param_name_lower in url_enum_constraints:
                enum_val = url_enum_constraints.pop(param_name_lower)
                converted['schema'] = {'type': 'string', 'enum': [enum_val.lower()]}
                converted['required'] = True
            oas3_params.append(converted)

    # Any URL query params not already covered by swagger params are added as new required params
    # (excluding 'handler' which was already filtered out above)
    for qk, qv in url_enum_constraints.items():
        oas3_params.append({
            'name': qk,
            'in': 'query',
            'required': True,
            'schema': {
                'type': 'string',
                'enum': [qv.lower()]
            },
            'description': f'Must be {qv.lower()}'
        })

    if oas3_params:
        oas3_op['parameters'] = oas3_params

    # Convert body param to requestBody
    if body_param:
        rb = build_request_body([body_param], method)
        if rb:
            oas3_op['requestBody'] = rb

    # Convert responses
    oas3_responses = {}
    for code, resp in op.get('responses', {}).items():
        oas3_responses[str(code)] = convert_response_schema(resp)

    if not oas3_responses:
        oas3_responses['200'] = {'description': 'Successful operation'}

    oas3_op['responses'] = oas3_responses

    return oas3_op


def validate_oas3(oas3):
    """Basic validation of the generated OAS3 spec."""
    issues = []

    # Check required top-level fields
    for field in ['openapi', 'info', 'paths']:
        if field not in oas3:
            issues.append(f'Missing required top-level field: {field}')

    # Check info fields
    for field in ['title', 'version']:
        if field not in oas3.get('info', {}):
            issues.append(f'Missing required info field: {field}')

    # Check for duplicate operationIds
    op_ids = {}
    for path, path_item in oas3.get('paths', {}).items():
        for method, op in path_item.items():
            if not isinstance(op, dict):
                continue
            op_id = op.get('operationId', '')
            if op_id:
                if op_id in op_ids:
                    issues.append(f'Duplicate operationId: {op_id} in {method.upper()} {path} and {op_ids[op_id]}')
                else:
                    op_ids[op_id] = f'{method.upper()} {path}'

    # Check for path params defined in path but not in parameters
    for path, path_item in oas3.get('paths', {}).items():
        path_params = set(re.findall(r'\{(\w+)\}', path))
        for method, op in path_item.items():
            if not isinstance(op, dict):
                continue
            param_names = {p['name'] for p in op.get('parameters', []) if p.get('in') == 'path'}
            missing_path_params = path_params - param_names
            if missing_path_params:
                issues.append(f'{method.upper()} {path}: path params {missing_path_params} not defined in parameters')

    # Check for dangling $refs in schemas
    import json
    oas3_text = json.dumps(oas3)
    all_refs = set(re.findall(r'#/components/schemas/([^"]+)', oas3_text))
    defined_schemas = set(oas3.get('components', {}).get('schemas', {}).keys())

    dangling = all_refs - defined_schemas
    for ref in sorted(dangling):
        issues.append(f'Dangling $ref: #/components/schemas/{ref}')

    return issues


def main():
    print(f'Reading {INPUT_FILE}...')
    with open(INPUT_FILE) as f:
        swagger_data = json.load(f)

    print(f'Swagger 2.0 spec has:')
    print(f'  - {len(swagger_data.get("paths", {}))} paths')
    print(f'  - {len(swagger_data.get("definitions", {}))} definitions')

    print('Converting to OpenAPI 3.1.0...')
    oas3 = convert_swagger_to_oas3(swagger_data)

    print(f'OpenAPI 3.1.0 spec has:')
    print(f'  - {len(oas3.get("paths", {}))} paths')
    print(f'  - {len(oas3.get("components", {}).get("schemas", {}))} schemas')

    print('Validating...')
    issues = validate_oas3(oas3)

    if issues:
        print(f'Validation issues ({len(issues)}):')
        for issue in issues[:30]:
            print(f'  - {issue}')
        if len(issues) > 30:
            print(f'  ... and {len(issues) - 30} more')
    else:
        print('No validation issues found!')

    print(f'Writing to {OUTPUT_FILE}...')
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(oas3, f, indent=4)

    print('Done!')
    return issues


if __name__ == '__main__':
    issues = main()
