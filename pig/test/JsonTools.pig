REGISTER /home/mufford/util/jyson-1.0.2.jar;
REGISTER '/home/mufford/json-toolbox/pig/JsonTools.py' using jython as jt;

data = LOAD '/sample-data/year=2016/month=3/day=2/RawEvent.1533b400b96.35ba013b.gz.parquet' USING parquet.pig.ParquetLoader();
only_json = FOREACH data GENERATE (chararray)json;
stats = FOREACH (GROUP only_json ALL) GENERATE COUNT(only_json.json) AS total_count;
-- example output:
-- (292878)

---------------------
-- JSON STRUCTURES --
---------------------
json_structures = FOREACH only_json GENERATE jt.GetUniqueSchemaAsString(json) AS keys;
structures = FOREACH (GROUP json_structures BY keys)
             GENERATE
                group AS json_schema,
                stats.total_count AS total_count,
                COUNT(json_structures.keys) AS occurrences,
                ((float)COUNT(json_structures.keys) / (float)stats.total_count) * 100 AS frequency;
sorted_structures = ORDER structures BY occurrences DESC;
top_structures = LIMIT sorted_structures 25; --> change or remove this limitation
DUMP top_structures;
-- or --
-- STORE sorted_structures INTO 'json_structure_7days' USING PigStorage();

-- example output:
-- (action;type;querystrings;querystrings.key;querystrings.key:val;querystrings.value;identifier;server;visitid;292878,165850,56.627674)

---------------
-- JSON KEYS --
---------------
json_structures = FOREACH only_json GENERATE jt.GetUniqueSchemaAsString(json) AS keys;
json_keys = FOREACH json_structures GENERATE flatten(TOKENIZE((chararray)keys, ';')) AS keys;
keys = FOREACH (GROUP json_keys BY keys)
       GENERATE
           group AS keys,
           stats.total_count AS total_count,
           COUNT(json_keys.keys) AS occurrences,
           ((float)COUNT(json_keys.keys) / (float)stats.total_count) * 100 AS frequency;
sorted_keys = ORDER keys BY occurrences DESC;
-- top_keys = FILTER sorted_keys BY frequency >= 25;
top_keys = LIMIT sorted_keys 25; --> change or remove this limitation
DUMP top_keys;
-- or --
-- STORE sorted_keys INTO 'json_structure_7days' USING PigStorage();

-- example output:
-- (guid,292878,292615,99.9102)

---------------
-- JSON DATA --
---------------
flatten_json = FOREACH only_json GENERATE jt.FlattenJson(json) AS data;
flatten_data = FOREACH flatten_json GENERATE FLATTEN(data) AS (attribute_name, attribute_path, attribute_value);
demo = LIMIT flatten_data 25;
DUMP demo;

-- example output:
-- (guid,guid,ccb46d96-234a-49cd-b0e0-5e53aeaff840)
-- TODO: grouping by key, return (unique paths), (top values), (frequency)
