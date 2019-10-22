# -*- coding: UTF-8 -*-
# Created by thpffcj on 2019/10/21.
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf

if __name__ == '__main__':
    spark = SparkSession.builder.appName("steam").getOrCreate()

    steam_data = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("/data/steam.csv")\
        .select("userId", "gameName", "behavior", "duration")

    # 200000
    # print(steam_data.count())

    def get_time(value):
        if value <= 10 and value >= 0:
            return "0 ~ 10小时"
        elif value <= 50:
            return "10 ~ 50小时"
        elif value <= 100:
            return "51 ~ 100小时"
        elif value <= 200:
            return "101 ~ 200小时"
        elif value <= 300:
            return "201 ~ 300小时"
        elif value <= 500:
            return "301 ~ 500小时"
        elif value > 500:
            return "大于500小时"
        else:
            return None

    grade_function_udf = udf(get_time, StringType())

    dota2_data = steam_data.filter(steam_data["behavior"] == "play").filter(steam_data["gameName"] == "Dota 2")\
        .withColumn("range", grade_function_udf(steam_data['duration']))
    dota2_group = dota2_data.groupBy("range").count()
    dota2_result = dota2_group.select("range", "count")\
        .withColumn("percent", dota2_group['count'] / dota2_data.count() * 100)

    team_fortress2_data = steam_data.filter(steam_data["behavior"] == "play").filter(steam_data["gameName"] == "Team Fortress 2")\
        .withColumn("range", grade_function_udf(steam_data['duration']))
    team_fortress2_group = team_fortress2_data.groupBy("range").count()
    team_fortress2_result = team_fortress2_group.select("range", "count")\
        .withColumn("percent", team_fortress2_group['count'] / team_fortress2_data.count() * 100)

    # team_fortress2_result.show()

    dota2_data.write.format("org.elasticsearch.spark.sql").option("es.nodes", "172.19.170.131:9200").mode(
            "overwrite").save("dota2/data")

    team_fortress2_data.write.format("org.elasticsearch.spark.sql").option("es.nodes", "172.19.170.131:9200").mode(
        "overwrite").save("team_fortress2/data")

    dota2_result.write.format("org.elasticsearch.spark.sql").option("es.nodes", "172.19.170.131:9200").mode(
        "overwrite").save("aggregation_dota2/aggregation")

    team_fortress2_result.write.format("org.elasticsearch.spark.sql").option("es.nodes", "172.19.170.131:9200").mode(
        "overwrite").save("aggregation_fortress2/aggregation")


    spark.stop()
