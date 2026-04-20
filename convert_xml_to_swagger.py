import xml.etree.ElementTree as ET
import json
import re
import os

def parse_contract_to_swagger(contract_xml_path, handlers_xml_path, output_file_path):
    try:
        contract_tree = ET.parse(contract_xml_path)
        contract_root = contract_tree.getroot()
        
        handlers_tree = ET.parse(handlers_xml_path)
        handlers_root = handlers_tree.getroot()
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return

    # Map Handler Name to Documentation and SampleUrl
    docs = {}
    sample_urls = {}
    for handler in handlers_root.findall('RequestHandler'):
        name = handler.get('Name')
        doc = handler.find('Documentation').text if handler.find('Documentation') is not None else ""
        url = handler.find('SampleUrl').text if handler.find('SampleUrl') is not None else ""
        docs[name] = doc
        sample_urls[name] = url

    swagger = {
        "swagger": "2.0",
        "info": {
            "title": "Spectra Logic S3 Server API",
            "description": "API documentation generated from request-handlers-contract.xml",
            "version": "1.0.0"
        },
        "host": "datapathdnsnameofappliance",
        "schemes": ["http", "https"],
        "basePath": "/",
        "paths": {},
        "definitions": {
            "JobToCreateApiBean": {
                "type": "object",
                "properties": {
                    "Objects": {
                        "type": "array",
                        "items": { "$ref": "#/definitions/S3ObjectToJobApiBean" }
                    }
                }
            },
            "S3ObjectToJobApiBean": {
                "type": "object",
                "properties": {
                    "Name": { "type": "string" },
                    "Size": { "type": "integer", "format": "int64" },
                    "Length": { "type": "integer", "format": "int64" },
                    "Offset": { "type": "integer", "format": "int64" },
                    "VersionId": { "type": "string" }
                }
            },
            "BlobIdsSpecification": {
                "type": "object",
                "properties": {
                    "BlobIds": { "type": "array", "items": { "type": "string", "format": "uuid" } },
                    "JobId": { "type": "string", "format": "uuid" }
                }
            },
            "S3ObjectsToDeleteApiBean": {
                "type": "object",
                "properties": {
                    "Quiet": { "type": "boolean" },
                    "ObjectsToDelete": {
                        "type": "array",
                        "items": { "$ref": "#/definitions/S3ObjectToDeleteApiBean" }
                    }
                }
            }
        }
    }

    # Process Types for definitions
    types_node = contract_root.find('.//Types')
    if types_node is not None:
        for type_node in types_node.findall('Type'):
            type_name = type_node.get('Name')
            short_name = type_name.split('.')[-1].replace('$', '.')
            
            if short_name in swagger['definitions'] and 'properties' in swagger['definitions'][short_name]:
                continue

            definition = {
                "type": "object",
                "properties": {}
            }
            
            elements = type_node.find('Elements')
            if elements is not None:
                for element in elements.findall('Element'):
                    el_name = element.get('Name')
                    el_type = element.get('Type')
                    el_component = element.get('ComponentType')
                    
                    prop = {}
                    if el_type == 'array':
                        prop['type'] = 'array'
                        if el_component:
                            if el_component.startswith('com.spectralogic.'):
                                prop['items'] = {"$ref": f"#/definitions/{el_component.split('.')[-1].replace('$', '.')}"}
                            else:
                                prop['items'] = {"type": map_java_type(el_component)}
                    elif el_type and el_type.startswith('com.spectralogic.'):
                        prop['$ref'] = f"#/definitions/{el_type.split('.')[-1].replace('$', '.')}"
                    else:
                        prop['type'] = map_java_type(el_type)
                    
                    definition['properties'][el_name] = prop
            
            enum_constants = type_node.find('EnumConstants')
            if enum_constants is not None and len(enum_constants.findall('EnumConstant')) > 0:
                definition['type'] = 'string'
                definition['enum'] = [ec.get('Name') for ec in enum_constants.findall('EnumConstant')]
                if 'properties' in definition: del definition['properties']

            swagger['definitions'][short_name] = definition

    # Process Request Handlers
    handlers_node = contract_root.find('.//RequestHandlers')
    for handler in handlers_node.findall('RequestHandler'):
        full_name = handler.get('Name')
        operation_id = full_name.split('.')[-1]
        classification = handler.get('Classification')
        
        request_node = handler.find('Request')
        if request_node is None: continue
        
        http_verb = request_node.get('HttpVerb').lower()
        
        # Build Base Path
        base_path = build_path(request_node, sample_urls.get(full_name))
        
        # Make path unique if needed
        final_path = base_path
        operation_param = request_node.get('Operation')
        
        # Check if this path+verb is already taken
        while final_path in swagger['paths'] and http_verb in swagger['paths'][final_path]:
             # It's already taken, we need to differentiate
             if operation_param and f"operation={operation_param}" not in final_path:
                 if '?' in final_path:
                     final_path += f"&operation={operation_param}"
                 else:
                     final_path += f"?operation={operation_param}"
             else:
                 # Even with operation it might conflict (like Replicate vs Create)
                 # Last resort: append operationId as a "pseudo-param" for uniqueness in Swagger
                 if '?' in final_path:
                     final_path += f"&handler={operation_id}"
                 else:
                     final_path += f"?handler={operation_id}"

        if final_path not in swagger['paths']:
            swagger['paths'][final_path] = {}

        parameters = []
        
        # Add Path Parameters from the BASE path (the one with {id})
        path_params = re.findall(r'\{(.*?)\}', base_path)
        for p in path_params:
            parameters.append({
                "name": p,
                "in": "path",
                "required": True,
                "type": "string"
            })
            
        # Add Query Parameters
        opt_params = request_node.find('OptionalQueryParams')
        if opt_params is not None:
            for p in opt_params.findall('Param'):
                parameters.append({
                    "name": p.get('Name'),
                    "in": "query",
                    "required": False,
                    "type": map_java_type(p.get('Type'))
                })
                
        req_params = request_node.find('RequiredQueryParams')
        if req_params is not None:
            for p in req_params.findall('Param'):
                parameters.append({
                    "name": p.get('Name'),
                    "in": "query",
                    "required": True,
                    "type": map_java_type(p.get('Type'))
                })

        # Manual Payload Mapping
        payload_ref = get_manual_payload(operation_id)
        if payload_ref:
            parameters.append({
                "name": "body",
                "in": "body",
                "required": True,
                "schema": payload_ref
            })

        responses = {}
        codes_node = handler.find('ResponseCodes')
        if codes_node is not None:
            for code_node in codes_node.findall('ResponseCode'):
                code = code_node.find('Code').text
                resp_types = code_node.find('ResponseTypes')
                resp_desc = "Successful operation" if code in ["200", "201", "204", "206"] else "Error"
                
                resp_obj = {"description": resp_desc}
                if resp_types is not None:
                    rtype = resp_types.find('ResponseType')
                    if rtype is not None:
                        type_str = rtype.get('Type')
                        comp_type = rtype.get('ComponentType')
                        if type_str == 'array':
                             if comp_type:
                                 if comp_type.startswith('com.spectralogic.'):
                                     resp_obj['schema'] = {
                                         "type": "array",
                                         "items": {"$ref": f"#/definitions/{comp_type.split('.')[-1].replace('$', '.')}"}
                                     }
                                 else:
                                     resp_obj['schema'] = {
                                         "type": "array",
                                         "items": {"type": map_java_type(comp_type)}
                                     }
                        elif type_str and type_str.startswith('com.spectralogic.'):
                             resp_obj['schema'] = {"$ref": f"#/definitions/{type_str.split('.')[-1].replace('$', '.')}"}
                        elif type_str and type_str != 'null':
                             resp_obj['schema'] = {"type": map_java_type(type_str)}
                
                responses[code] = resp_obj

        operation = {
            "operationId": operation_id,
            "summary": operation_id,
            "description": docs.get(full_name, ""),
            "parameters": parameters,
            "responses": responses,
            "tags": [classification]
        }
        
        swagger['paths'][final_path][http_verb] = operation

    with open(output_file_path, 'w') as f:
        json.dump(swagger, f, indent=4)

def map_java_type(java_type):
    if not java_type: return "string"
    mapping = {
        'java.lang.String': 'string',
        'java.util.UUID': 'string',
        'long': 'integer',
        'int': 'integer',
        'boolean': 'boolean',
        'double': 'number',
        'float': 'number',
        'java.lang.Integer': 'integer',
        'java.lang.Long': 'integer',
        'java.lang.Boolean': 'boolean',
        'java.lang.Class': 'string'
    }
    return mapping.get(java_type, "string")

def build_path(request_node, sample_url):
    if sample_url:
        match = re.search(r'datapathdnsnameofappliance(/.+)', sample_url)
        if match:
            path = match.group(1).split('?')[0].split('[')[0]
            path = re.sub(r'\{unique identifier or attribute\}', '{id}', path)
            path = re.sub(r'\{text\}', '{name}', path)
            if not path.startswith('/'): path = '/' + path
            return path

    resource = request_node.get('Resource')
    if resource:
        res_path = resource.lower()
        if request_node.get('IncludeIdInPath') == 'true':
            return f"/_rest_/{res_path}/{{id}}"
        return f"/_rest_/{res_path}/"
    
    return "/"

def get_manual_payload(operation_id):
    # known payloads
    mapping = {
        "CreatePutJobRequestHandler": {"$ref": "#/definitions/JobToCreateApiBean"},
        "CreateGetJobRequestHandler": {"$ref": "#/definitions/JobToCreateApiBean"},
        "CreateVerifyJobRequestHandler": {"$ref": "#/definitions/JobToCreateApiBean"},
        "StageObjectsJobRequestHandler": {"$ref": "#/definitions/JobToCreateApiBean"},
        "GetBlobPersistenceRequestHandler": {"$ref": "#/definitions/BlobIdsSpecification"},
        "VerifyPhysicalPlacementForObjectsRequestHandler": {"$ref": "#/definitions/JobToCreateApiBean"},
        "GetPhysicalPlacementForObjectsRequestHandler": {"$ref": "#/definitions/JobToCreateApiBean"},
        "DeleteObjectsRequestHandler": {"$ref": "#/definitions/S3ObjectsToDeleteApiBean"},
        "ReplicatePutJobRequestHandler": {"type": "object"}
    }
    return mapping.get(operation_id)

if __name__ == "__main__":
    base_path = "/Users/ashapillai/devRepo/redline/product/frontend/server/src/main/resources"
    contract_xml = os.path.join(base_path, "request-handlers-contract.xml")
    handlers_xml = os.path.join(base_path, "request-handlers.xml")
    output_swagger = "/Users/ashapillai/devRepo/redline/product/frontend/swagger.json"
    
    parse_contract_to_swagger(contract_xml, handlers_xml, output_swagger)
    print(f"Successfully fixed swagger.json at {output_swagger}")
