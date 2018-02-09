"""
  To get this example working: (Tested on Python 3.6)

  1. Upload this script using the upload_resource bash script in the repo
  2. Download nltk package from https://pypi.python.org/pypi/nltk
  3. Download stanford package from https://nlp.stanford.edu/software/stanford-ner-2017-06-09.zip or https://nlp.stanford.edu/software/CRF-NER.shtml#Download
  4. Extract the stanford package and upload the 'stanford-ner.jar' and 'classifiers/english.all.3class.distsim.crf.ser.gz' package
  5. Upload nltk to blob store
  6. Define the Custom Spark job similar to json below


    {
      "type" : "custom_spark_job",
      "id" : "custom_entity_python_extraction",
      "resourceName" : "test_nltk_ner_rdd.py",
      "sparkConfig" : [ {
        "key" : "spark.executor.memory",
        "value" : "2g"
      }, {
        "key" : "spark.driver.memory",
        "value" : "2g"
      }, {
        "key" : "spark.logConf",
        "value" : "true"
      }, {
        "key" : "spark.eventLog.enabled",
        "value" : "true"
      }, {
        "key" : "spark.eventLog.compress",
        "value" : "true"
      }, {
        "key" : "spark.scheduler.mode",
        "value" : "FAIR"
      } ],
      "submitArgs" : [ ],
      "javaOptions" : [ ],
      "verboseReporting" : true,
      "files" : [ "english.all.3class.distsim.crf.ser.gz", "stanford-ner.jar" ],
      "pythonFiles" : [ "nltk-3.2.5.tar.gz" ],
      "deleteOnExit" : true
    }
"""

from pyspark.sql import SparkSession
from pyspark.files import SparkFiles
from nltk.tag import StanfordNERTagger
from nltk.tokenize import word_tokenize
from pyspark.sql.types import StringType, StructType, ArrayType, StructField, Row
from collections import OrderedDict
import nltk

nltk.download()

spark = SparkSession \
    .builder \
    .appName("Python NLTK example") \
    .getOrCreate()

classifier_path = SparkFiles.get("english.all.3class.distsim.crf.ser.gz")
ner_jar = SparkFiles.get("stanford-ner.jar")
st = StanfordNERTagger(classifier_path, ner_jar, encoding='utf-8')


def classify_text(row):
    text = row["body_t"]
    # Collapse array to single value field
    all_text = str.join(' ', text)
    tokenized_text = word_tokenize(all_text)
    classified_text = st.tag(tokenized_text)
    classifiers_dict = dict()
    for word, clz in classified_text:
        if clz in classifiers_dict:
            classifiers_dict[clz].add(word)
        else:
            classifiers_dict[clz] = {word}
    # Ignore un-identified other entities
    if 'O' in classifiers_dict:
        del classifiers_dict['O']
    ordered_dict = OrderedDict()
    ordered_dict["location_ss"] = list(classifiers_dict.get("LOCATION", {}))
    ordered_dict["organization_ss"] = list(classifiers_dict.get("ORGANIZATION", {}))
    ordered_dict["person_ss"] = list(classifiers_dict.get("PERSON", {}))
    ordered_dict["id"] = row["id"]
    return ordered_dict


read_opts = {"collection": "cnn", "fields": "id, body_t", "max_rows": "10", "query": "body_t:[* TO *]"}
df = spark.read.format("solr").options(**read_opts).load()

cnn_rdd = df.rdd.map(lambda r: Row(**classify_text(r.asDict())))

schema = StructType(
    [
        StructField("location_ss", ArrayType(StringType())),
        StructField("organization_ss", ArrayType(StringType())),
        StructField("person_ss", ArrayType(StringType())),
        StructField("id", StringType())
    ]
)

newdf = spark.createDataFrame(cnn_rdd, schema)
# newdf.cache

spark.sparkContext._jvm.com.lucidworks.spark.util.DatasetLoader.sendAtomicAddsToSolr(newdf._jdf, "cnn", "id", "localhost:9983/lwfusion/4.0.0-SNAPSHOT/solr")

spark.stop()
