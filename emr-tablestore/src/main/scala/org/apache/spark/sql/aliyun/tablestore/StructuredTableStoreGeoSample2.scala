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
      .schema("pk1 STRING, val_long1 LONG, val_geo STRING")
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
      .option("max.split.count", 16)
      .load()
      .createTempView("search_view")

    val geoDistanceQuery = spark.sql("""SELECT COUNT(*) FROM search_view WHERE val_geo = '{"centerPoint":"6.530045901643962,9.05358919674954", "distanceInMeter": 3000.0}' LIMIT 100""")
    geoDistanceQuery.show()

    val geoBoundingBoxQuery = spark.sql("""SELECT COUNT(*) FROM search_view WHERE val_geo = '{"topLeft":"6.257664116603074,9.1595116589601", "bottomRight": "6.153593333442616,9.25968497923747"}' """)
    geoBoundingBoxQuery.show()
//
    val geoPolygonQuery = spark.sql("""SELECT COUNT(*) FROM search_view WHERE val_geo = '{"points":["6.530045901643962,9.05358919674954", "6.257664116603074,9.1595116589601", "6.160393397574926,9.256517839929597", "6.16043846779313,9.257192872563525"]}' """)
    geoPolygonQuery.show()
  }
}
