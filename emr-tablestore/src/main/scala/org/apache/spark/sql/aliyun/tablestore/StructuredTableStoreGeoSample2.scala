package org.apache.spark.sql.aliyun.tablestore

import java.util.UUID

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object StructuredTableStoreGeoSample2 extends Logging {
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
      .schema("pk1 STRING, val_long1 STRING, val_geo STRING")
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
      .option("search.index.name", "geo_table_index")
      .load()
      .createTempView("search_view")

    val geoDistanceQuery = spark.sql("""SELECT COUNT(*) FROM search_view WHERE val_geo = '{"centerPoint":"6,9", "distanceInMeter": 10000}' LIMIT 100""")
    //    val geoDistanceQuery = spark.sql("""SELECT COUNT(*) FROM search_view WHERE pk1 = '1420wurpoljjkhjkkigeca99544225' LIMIT 100""")
    geoDistanceQuery.show()

    //    val geoBoundingBoxQuery = spark.sql("""SELECT * FROM search_view WHERE geo = '{"topLeft":"8,0", "bottomRight": "0,10"}' """)
    //    geoBoundingBoxQuery.show()

    //    val geoPolygonQuery = spark.sql("""SELECT * FROM search_view WHERE geo = '{"points":["5,0", "5,1", "6,1", "6,10"]}' """)
    //    geoPolygonQuery.show()
  }
}
