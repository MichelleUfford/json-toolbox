from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
from pandas.io.json import json_normalize
from .. import JsonToMap

json_to_map = udf(lambda in_str: jsonToMap(in_str), MapType(StringType(), IntegerType()))

# load raw logs
sample_data = sqlContext \
    .table("default.sample_data") \
    .filter("dateint = 20160702 and hour = 0")

# test
test = sample_data \
    .select(
        sample_data.record_id.alias('run_id')
        , sample_data.record_type
        , sample_data.json
        , json_to_map(sample_data.json).alias('json_map')
) \
    .filter('record_type = "run"')

test.take(1)
