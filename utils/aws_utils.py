def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config["mysql_conf"]["hostname"]
    port = mysql_config["mysql_conf"]["port"]
    database = mysql_config["mysql_conf"]["database"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)

def get_redshift_jdbc_url(redshift_config: dict):
    host = redshift_config["redshift_conf"]["host"]
    port = redshift_config["redshift_conf"]["port"]
    database = redshift_config["redshift_conf"]["database"]
    username = redshift_config["redshift_conf"]["username"]
    password = redshift_config["redshift_conf"]["password"]
    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)

def write_into_redshift(dim_df, app_secret, app_conf, tablename):
    jdbc_url = get_redshift_jdbc_url(app_secret)
    dim_df.write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", jdbc_url) \
        .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
        .option("forward_spark_s3_credentials", "true") \
        .option("dbtable", tablename) \
        .mode("overwrite") \
        .save()

