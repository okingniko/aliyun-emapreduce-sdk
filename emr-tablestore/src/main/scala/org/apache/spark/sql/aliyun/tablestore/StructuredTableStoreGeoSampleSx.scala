package org.apache.spark.sql.aliyun.tablestore

import java.util.UUID

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object StructuredTableStoreGeoSampleSx extends Logging {
  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      System.err.println(
        "Usage: StructuredTableStoreWordCount <ots-instanceName>" +
          "<ots-tableName> <ots-tunnelId> <access-key-id> <access-key-secret> <ots-endpoint>" +
          "<max-offsets-per-channel> [<checkpoint-location>]"
      )
    }

    val Array(
    instanceName,
    tableName,
    tunnelId,
    accessKeyId,
    accessKeySecret,
    endpoint,
    maxOffsetsPerChannel,
    _*
    ) = args

    val checkpointLocation =
      if (args.length > 7) args(7) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark =
      SparkSession.builder.appName("TableStoreWordCount").master("local[16]").getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
    spark.read
      .schema("pk1 STRING, val_long1 LONG, val_geo STRING,val_keyword1 STRING,val_keyword2 STRING")
      .format("tablestore")
      .option("instance.name", instanceName)
      .option("table.name", tableName)
      .option("tunnel.id", tunnelId)
      .option("endpoint", endpoint)
      .option("access.key.id", accessKeyId)
      .option("access.key.secret", accessKeySecret)
      .option("maxOffsetsPerChannel", maxOffsetsPerChannel) // default 10000

      //      .option(
      //        "catalog",
      //        """{"columns": {"pk1": {"type": "string"} , "val_long1": {"type": "long"}, "val_geo": {"type": "string"}
      //          |}}""".stripMargin
      //      )

      .option("search.index.name", "wenxian_searchIndex_spark_test_index2")
      .load()
      .createTempView("search_view")
    //geo  in  not null  null

//        val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where pk1 like '0000pppeee444t%'  LIMIT 100   """)
    //    val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where val_long1<=3697900  LIMIT 100   """)
    //        val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where  val_keyword1 in ('111','b11')   LIMIT 100   """.stripMargin)
    //    val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where  val_keyword2 is null   LIMIT 100   """.stripMargin)
//        val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where pk1 like '0000pppeee4444%' and (val_long1 >= 3697900 and val_long1 <= 98350700) LIMIT 100   """.stripMargin)
//        val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where pk1 like '0000pppeee4444%' and (val_long1 >= 3697900 or val_long1 <= 98350700) LIMIT 100   """.stripMargin)
//        val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where pk1 like '0000pppeee4444%' and (val_long1 >= 3697900 ) LIMIT 100   """.stripMargin)
        val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where  val_geo = '{"centerPoint":"57.188941998439105,35.77072682936891", "distanceInMeter": 10000}' and val_long1 >= 1231 and val_long1 <=321 and val_long1 = 222  LIMIT 100   """.stripMargin)
//    val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where val_long1 =  123  or val_keyword2 is null   and   val_long1 != 75158600 and ( val_long1  != 123  and val_long1 =456 and val_long1 = 789) LIMIT 100   """)
    //    val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where  pk1 LIKE 'vikn%' and val_long1 in (27306297,75158600,67262582)  LIMIT 100   """)
    //    val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view WHERE  pk1 LIKE 'viknqvwz05578%' and val_long1 is not null LIMIT 100   """)
    //    val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view WHERE val_long1 in (23297075,31439381) LIMIT 100   """)
    //        val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view WHERE pk1 LIKE '9%'  and val_long1 <= 31439381 and   val_long1 >= 23297075   and val_geo = '{"centerPoint":"6,9", "distanceInMeter": 10000}' LIMIT 100   """)
    //    val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view WHERE val_geo = '{"centerPoint":"3,0", "distanceInMeter": 10000}' LIMIT 100   """)
    //        val geoDistanceQuery = spark.sql("""SELECT COUNT(*) FROM search_view WHERE pk1 = '1420wurpoljjkhjkkigeca99544225' LIMIT 100""")
    //    val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where  val_geo = '{"centerPoint":"6,9", "distanceInMeter": 100000}'   LIMIT 100   """)
    //    val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where  val_long1 >= 10 and val_long1 !=3 or val_geo = '{"topLeft":"3,0", "bottomRight": "0,3"}'   LIMIT 100   """)
    //    val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where val_geo = '{"centerPoint":"6,9", "distanceInMeter": 100000}'   LIMIT 100   """)
    geoDistanceQuery.show(false)

    //            val geoBoundingBoxQuery = spark.sql("""SELECT * FROM search_view WHERE val_geo = '{"topLeft":"3,0", "bottomRight": "0,3"}' """)
    //    geoBoundingBoxQuery.show(false)

    //    val geoPolygonQuery = spark.sql("""SELECT * FROM search_view WHERE val_geo = '{"points":["5,0", "5,1", "6,1", "6,10"]}' """)
    //    geoPolygonQuery.show(false)
  }
}
