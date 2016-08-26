#!/usr/bin/env python
# -*- coding: utf-8 -*-

# JsonTools.py
# A series of Pig UDFs for parsing very ugly JSON documents that may contain a wide variety of schemas
# mufford 20160215

from com.xhaus.jyson import JysonCodec as json
# Note: the use of ^ module requires that you register the jyson jar prior to registering this script, i.e.
#     REGISTER /home/mufford/lib/jyson-1.0.2.jar
#     REGISTER '/home/mufford/json-toolbox/pig/JsonTools.py' using jython as jt;

# retrieves a list of unique keys
def get_unique_schema(input, include_value_of_key=True):

    key_paths = {}
    unique_keys = []

    # recursive shredding of json
    def _shred(input, prefix=''):
        if isinstance(input, dict):
            for k, v in input.iteritems():
                # make case-insensitive
                key = unicode(k).lower()
                value = unicode(v).lower()
                if prefix:
                    path = prefix + '.' + key
                else:
                    path = key
                key_paths[path] = key

                # include values for key='key'
                # example: {"Key":"e_id","Value":"x.y.z"}
                if include_value_of_key == True and key == 'key':
                    path = prefix + '.key:' + value
                    key_paths[path] = key

                if (isinstance(v, dict) or
                        isinstance(v, list) or
                        isinstance(v, tuple)
                    ):
                    _shred(v, path)
        elif isinstance(input, list) or isinstance(input, tuple):
            for enum, item in enumerate(input):
                _shred(item, prefix)

    _shred(input)

    for key, value in key_paths.iteritems():
        unique_keys.append((key))

    return unique_keys


# searches for the value of a specific key
def search_by_key(input, key, search_value_of_key=True):

    output = []
    search_key = unicode(key).lower()

    # recursive search to support complex json structures
    def _search(input, key, search_keys=True):
        if isinstance(input, dict):
            for k, v in input.items():
                # make case-insensitive
                key = unicode(k).lower()
                value = unicode(v).lower()
                if key == search_key:
                    output.append(value)
                    break
                # include values for key='key'
                # example: {"Key":"e_id","Value":"x.y.z"}
                elif search_keys == True and key == 'key':
                    if value == search_key:
                        if 'value' in input.keys():
                            output.append(unicode(input['value']).lower())
                        else:
                            break
                elif (isinstance(v, dict) or
                          isinstance(v, list) or
                          isinstance(v, tuple)
                      ):
                    _search(v, key, search_keys)
        elif isinstance(input, list) or isinstance(input, tuple):
            for item in input:
                _search(item, key, search_keys)

    _search(input, key, search_value_of_key)
    return output


# outputs a JSON structure as a tuple
@outputSchema("output:tuple(key:chararray)")
def GetUniqueSchema(input, include_value_of_key=True):
    data = json.loads(input)
    if data is None: return None
    try:
        keys = get_unique_schema(data, include_value_of_key)
    except: return None
    else: return keys


# outputs a JSON structure as a string
@outputSchema("output:chararray")
def GetUniqueSchemaAsString(input, include_value_of_key=True):
    data = json.loads(input)
    if data is None: return None
    try:
        keys = ';'.join(sorted(get_unique_schema(data, include_value_of_key)))
    except: return None
    else: return keys


# outputs the value of a provided key regardless of its location in a nested structure
@outputSchema("output:chararray")
def GetValue(input, key, search_value_of_key=True):
    data = json.loads(input.lower())
    if data is None: return '<!invalid input>'
    try:
        results = search_by_key(data, key, search_value_of_key)
        value = ','.join(map(str,results))
    except: return '<!parsing error>'
    else: return value