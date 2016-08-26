from pandas.io.json import json_normalize
import json
import re
# from collections import OrderedDict


def json_to_map(json_in):

    json_dict = json.loads(json_in)
    json_flat = json_normalize(json_dict)
    json_map = json_flat.to_dict('records')[0]
    json_clean = {}
    regex_underscore = re.compile(r'(\s|-)')
    regex_remove = re.compile(r'(:|\(|\))')

    for key in json_map:
        clean_key = re.sub(regex_remove, '', re.sub(regex_underscore, '_', key.lower()))
        json_clean[clean_key] = json_map[key]

    return json_clean


# try again, this time with only modules already preinstalled on Spark cluster
def json_to_map_v2(json_str, path=None):

    try:
        json_dict = json.loads(json_str)
    except:
        return 'error: invalid json document'

    regex_underscore = re.compile(r'(\s|-)')
    regex_remove = re.compile(r'(:|\(|\))')

    flatten_map = {}

    # recursive shredding of json
    try:
        def _shred(input, prefix=''):
            if isinstance(input, dict):
                for k, v in input.iteritems():

                    # clean up the key
                    key = re.sub(regex_remove, '', re.sub(regex_underscore, '_', k.lower().strip()))

                    if prefix:
                        path = prefix + '.' + key
                    else:
                        path = key

                    if (isinstance(v, dict) or
                            isinstance(v, list) or
                            isinstance(v, tuple)
                        ):
                        _shred(v, path)
                    else:
                        flatten_map[path] = v
            elif isinstance(input, list) or isinstance(input, tuple):
                for enum, item in enumerate(input):
                    print(input)
                    _shred(item, prefix)

        if path:
            _shred(json_dict[path])
        else:
            _shred(json_dict)

        return flatten_map
    except:
        return "error: unable to parse json"


# json_to_map = udf(lambda str_in: jsonToMap(str_in), MapType(StringType(), IntegerType()))
# print(jsonToMap(sample_json, "totalCounters"))