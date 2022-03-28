import findspark, socket
findspark.init('/usr/local/spark')
from pyspark.sql import SparkSession
from pyspark import SparkConf

def get_spark_session(app_name: str, conf: SparkConf, master: str, configuracion):
    conf.setMaster(master)
    for key, value in configuracion.items():
        conf.set(key, value)    
    return SparkSession.builder.appName(app_name).config(conf=conf).getOrCreate()

config_standalone = {
    "spark.sql.legacy.setCommandRejectsSparkCoreConfs": "false"
    ,"spark.driver.host": socket.gethostbyname(socket.gethostname())
}

master_standalone = 'spark://sk-spark-cluster-master-0.sk-spark-cluster-headless.starterkit.svc.cluster.local:7077'
name = "app_" + socket.gethostbyname + "_" + str(socket.gethostbyname(socket.gethostname()))

spark_conf = SparkConf()
spark = get_spark_session(name, spark_conf, master_standalone, config_standalone)
sc = spark.sparkContext
