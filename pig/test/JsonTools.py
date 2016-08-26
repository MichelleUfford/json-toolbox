#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################################################################
# LOCAL DEV & TEST ONLY
# The code below here is only used for local dev & test; please do not include in a production deployment
#########################################################################################################################
import json

# ignore @outputSchema syntax unique to Pig UDFs
if __name__ != '__lib__':
    def outputSchema(dont_care):
        def wrapper(func):
            def inner(*args, **kwargs):
                return func(*args, **kwargs)
            return inner
        return wrapper

#########################################################################################################################
# PRODUCTION CODE
# The code below here is what should be used in a production environment
#########################################################################################################################

#!/usr/bin/env python
# -*- coding: utf-8 -*-

# JsonTools.py
# A series of Pig UDFs for parsing very ugly JSON documents that may contain a wide variety of schemas
# mufford 20160211

#from com.xhaus.jyson import JysonCodec as json
# Note: the use of ^ module requires that you register the jyson jar prior to registering this script, i.e.
#     REGISTER /home/mufford/lib/jyson-1.0.2.jar
#     REGISTER '/home/mufford/json-toolbox/pig/JsonTools.py' using jython as jt;

########################################################################################################################
# internal functions
########################################################################################################################
# retrieves a list of unique keys
def _get_unique_schema(input, include_value_of_key=True):

    key_paths = {}
    unique_keys = []

    # recursive shredding of json
    def _shred_schema(input, prefix=''):
        try:
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
                        _shred_schema(v, path)
            elif isinstance(input, list) or isinstance(input, tuple):
                for enum, item in enumerate(input):
                    _shred_schema(item, prefix)
        except:
            pass

    _shred_schema(input)

    for key, value in key_paths.iteritems():
        unique_keys.append((key))
    return unique_keys


# searches for the value of a specific key
def _search_by_key(input, key, search_value_of_key=True):

    output = []
    search_key = unicode(key).lower()

    # recursive search to support complex json structures
    def _search(input, key, search_keys=True):
        try:
            if isinstance(input, dict):
                for k, v in input.items():
                    try:
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
                    except:
                        continue
            elif isinstance(input, list) or isinstance(input, tuple):
                for item in input:
                    _search(item, key, search_keys)
        except:
            pass

    _search(input, key, search_value_of_key)
    return output


# parses a JSON document into a flat structure for data profiling
def _parse_json(input, include_value_of_key=True):
    try:

        key_paths = {}
        unique_keys = []
        output = []

        # recursive shredding of json
        def _shred_json(input, prefix=''):
            try:
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
                        if key == 'key':
                            path = prefix + '.key:' + value
                            key_paths[path] = key

                        # more nesting, continue recursion
                        if (isinstance(v, dict) or
                                isinstance(v, list) or
                                isinstance(v, tuple)
                            ):
                            _shred_json(v, path)
                        else:
                            if key == 'key':
                                if 'value' in input.keys():
                                    output.append((value, path, input['value']))
                            elif key != 'value':
                                output.append((key, path, value))
                elif isinstance(input, list) or isinstance(input, tuple):
                    for enum, item in enumerate(input):
                        _shred_json(item, prefix)
            except:
                pass

        _shred_json(input)
        return output
    except:
        return [(u'<!error!>',u'<!error!>',u'<!error!>')]

########################################################################################################################
# public Pig UDFs
########################################################################################################################

# outputs a JSON structure as a tuple
@outputSchema("output:tuple(key:chararray)")
def GetUniqueSchema(input, include_value_of_key=True):
    data = json.loads(input)
    if data is None: return None
    try:
        keys = _get_unique_schema(data, include_value_of_key)
    except: return None
    else: return keys


# outputs a JSON structure as a string
@outputSchema("output:chararray")
def GetUniqueSchemaAsString(input, include_value_of_key=True):
    data = json.loads(input)
    if data is None: return None
    try:
        keys = ';'.join(sorted(_get_unique_schema(data, include_value_of_key)))
    except: return None
    else: return keys


# outputs the value of a provided key regardless of its location in a nested structure
@outputSchema("output:chararray")
def GetValue(input, key, search_value_of_key=True):
    data = json.loads(input.lower())
    if data is None: return '<!invalid input>'
    try:
        results = _search_by_key(data, key, search_value_of_key)
        value = ','.join(map(str,results))
    except: return '<!parsing error>'
    else: return value


# outputs the value of a provided key regardless of its location in a nested structure
@outputSchema("output:bag{attribute:tuple(attribute_name:chararray, attribute_path:chararray, attribute_value:chararray)}")
def FlattenJson(input):
    data = json.loads(input.lower())
    if data is None: return [(u'<!error!>',u'<!error!>',u'<!error!>')]
    return _parse_json(data)


#########################################################################################################################
# LOCAL DEV & TEST ONLY
# The code below here is only used for local dev & test; please do not include in a production deployment
#########################################################################################################################

import os
import pprint
sample_data_path = os.path.join(os.path.dirname(__file__), 'data_samples', 'error.json')
sample_data = open(sample_data_path, 'r').read()
output = FlattenJson(sample_data)
pprint.pprint(output)