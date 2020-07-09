package org.apache.spark.sql.aliyun.tablestore

import java.util.UUID

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object StructuredTableStoreGeoSampleForUnhandledFilter extends Logging {
  def main(args: Array[String]): Unit = {
    if (args.length < 9) {
      System.err.println(
        "Usage: StructuredTableStoreWordCount <ots-instanceName>" +
          "<ots-tableName> <ots-tunnelId> <access-key-id> <access-key-secret> <ots-endpoint>" +
          "<max-offsets-per-channel> [<checkpoint-location>] <push-down-rang-long> <push-down-rang-string>"
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
    pushdownRangeLong,
    pushdownRangeString,
    _*
    ) = args

    val checkpointLocation =
      if (args.length > 9) args(9) else "/tmp/temporary-" + UUID.randomUUID.toString

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
      .option("search.index.name", "wenxian_searchIndex_spark_test_index2")
      .option("push.down.range.long", pushdownRangeLong)
      .option("push.down.range.string", pushdownRangeString)
      .load()
      .createTempView("search_view")

    //    //and的时候推不推long or的时候推不推由 参数决定
    //and 有geo   有rangelong  有term                   geo term必然下推            不推rangelong取决于参数
    //        val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where val_geo = '{"centerPoint":"3,0", "distanceInMeter": 100000}'  and val_long1 >= 27494400 LIMIT 100   """.stripMargin)
    //
    //and 有geo   有rangelong  有isnull                        不推rangelong取决于参数
    //    val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where val_geo = '{"centerPoint":"3,0", "distanceInMeter": 100000}'  and val_long1 >= 27494400  and val_long1 in(37691900,55747100) LIMIT 100   """.stripMargin)

    //
    //    //and 有geo   有rangelong   有 in                  不推rangelong取决于参数
    //    val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where val_geo = '{"centerPoint":"3,0", "distanceInMeter": 10000}' and val_long1 > 3697900 val_long1 in ( 3697900 , 8350700)  LIMIT 100   """.stripMargin)
    //
    //    //and 有geo   有rangelong 有is not equal   不推rangelong取决于参数
    //    val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where val_geo = '{"centerPoint":"3,0", "distanceInMeter": 100000}'  and val_long1 >= 27494400  and val_long1 in(37691900,55747100) and val_long1 != 37691900 LIMIT 100   """.stripMargin)

    //
    //    //and 有rangelong  无geo   不推rangelong取决于参数
    val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where val_long1 >= 27494400  and val_long1 in(37691900,55747100) and val_long1 != 37691900 LIMIT 100   """.stripMargin)
      .hint("pushdownRangeLong", pushdownRangeLong.toBoolean) .hint("pushdownRangeString", pushdownRangeString.toBoolean)

    //
    //or rangelong                                              必须全下推
    //    val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where  val_long1 < 8350700 or  val_long1 > 3697900  LIMIT 100   """.stripMargin)

    //or geo                                                    报错
    //    val geoDistanceQuery = spark.sql("""  SELECT * FROM search_view where  val_geo = '{"centerPoint":"3,0", "distanceInMeter": 10000}'  or  val_long1 > 3697900  LIMIT 100   """.stripMargin)

    geoDistanceQuery.show(false)

    //            val geoBoundingBoxQuery = spark.sql("""SELECT * FROM search_view WHERE val_geo = '{"topLeft":"3,0", "bottomRight": "0,3"}' """)
    //    geoBoundingBoxQuery.show(false)

    //    val geoPolygonQuery = spark.sql("""SELECT * FROM search_view WHERE val_geo = '{"points":["5,0", "5,1", "6,1", "6,10"]}' """)
    //    geoPolygonQuery.show(false)
  }
}
