from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
import yaml
import os.path
import utils.aws_utils as ut

if __name__ == '__main__':

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"])\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    src_list = app_conf['source_list']
    for src in src_list:
        src_conf = app_conf[src]
        src_loc = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src
        if src == 'SB':
            jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                          "lowerBound": "1",
                          "upperBound": "100",
                          "dbtable": src_conf["mysql_conf"]["dbtable"],
                          "numPartitions": "2",
                          "partitionColumn": src_conf["mysql_conf"]["partition_column"],
                          "user": app_secret["mysql_conf"]["username"],
                          "password": app_secret["mysql_conf"]["password"]
                           }
            # print(jdbcParams)

            # use the ** operator/un-packer to treat a python dictionary as **kwargs
            print("\nReading data from MySQL DB using SparkSession.read.format(),")
            txnDF = spark\
                .read.format("jdbc")\
                .option("driver", "com.mysql.cj.jdbc.Driver")\
                .options(**jdbc_params)\
                .load()\
                .withColumn('ins_date', current_date())

            # add the current date to the above df and then you write it
            txnDF.show()
            txnDF\
                .write.mode("append")\
                .partitionBy('ins_date') \
                .format("parquet") \
                .save(src_loc)

        elif src == 'OL':
            # put the code here to pull rewards data from SFTP server and write it to s3
            ol_txn_df = spark.read\
                .format("com.springml.spark.sftp")\
                .option("host", app_secret["sftp_conf"]["hostname"])\
                .option("port", app_secret["sftp_conf"]["port"])\
                .option("username", app_secret["sftp_conf"]["username"])\
                .option("pem", os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"]))\
                .option("fileType", "csv")\
                .option("delimiter", "|")\
                .load(src_conf["sftp_conf"]["directory"] + "/receipts_delta_GBR_14_10_2017.csv")\
                .withColumn('ins_date', current_date())

            ol_txn_df.show()
            ol_txn_df\
                .write.mode("append")\
                .partitionBy('ins_date') \
                .format("parquet") \
                .save(src_loc)

        elif src == 'ADDR':
            cust_addr = spark \
                .read \
                .format("com.mongodb.spark.sql.DefaultSource") \
                .option("database", src_conf["mongodb_config"]["database"]) \
                .option("collection", src_conf["mongodb_config"]["collection"]) \
                .load()
            cust_addr.show()
            cust_addr.printSchema()

            cust_addr = cust_addr.select(cust_addr['consumer_id'],
                             cust_addr['mobile-no'].alias('mobile-no'),
                             cust_addr['address.street'].alias('street'),
                             cust_addr['address.city'].alias('city'),
                             cust_addr['address.state'].alias('state'),
                             current_date().alias('ins_date'))

            cust_addr.show()
            cust_addr \
                .write.mode("append") \
                .partitionBy('ins_date') \
                .format("parquet") \
                .save(src_loc)

        elif src == 'CP':
            cp_df = spark.read \
                .option('header', "true") \
                .option('delimiter', '|') \
                .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + src_conf["filename"])\
                .withColumn('ins_date', current_date())

            cp_df.show(5, False)

            cp_df \
                .write.mode("append") \
                .partitionBy('ins_date') \
                .format("parquet") \
                .save(src_loc)


# spark-submit --master yarn --packages "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1" com/pg/source_data_loading.py
