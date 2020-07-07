/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.aliyun.tablestore

import java.util

import com.alicloud.openservices.tablestore.ecosystem.{TablestoreSplit, Filter => OTSFilter}
import com.alicloud.openservices.tablestore.model.{Row => TSRow, _}
import com.alicloud.openservices.tablestore.{ClientConfiguration, SyncClient}
import com.aliyun.openservices.tablestore.hadoop._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde2.SerDeException
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{Filter, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


class TableStoreRelation(
                          parameters: Map[String, String],
                          userSpecifiedSchema: Option[StructType])(@transient val sqlContext: SQLContext)
  extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation
    with Serializable
    with Logging {

  val accessKeyId: String = parameters("access.key.id")
  val accessKeySecret: String = parameters("access.key.secret")
  val endpoint: String = parameters("endpoint")
  val tbName: String = parameters("table.name")
  val instanceName: String = parameters("instance.name")
  val batchUpdateSize: String = parameters.getOrElse("batch.update.size", "0")

  val computeMode: String = parameters.getOrElse("compute.mode", "KV")
  val maxSplitsCount: Int = parameters.getOrElse("max.split.count", "1000").toInt
  val splitSizeInMbs: Long = parameters.getOrElse("split.size.mbs", "100").toLong
  val searchIndexName: String = parameters.getOrElse("search.index.name", "")
  var hadoopConf: Configuration = null;

  override def schema: StructType =
    userSpecifiedSchema.getOrElse(TableStoreCatalog(parameters).schema)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    hadoopConf = new Configuration()
    hadoopConf.set(TableStoreInputFormat.TABLE_NAME, tbName)
    if (!searchIndexName.isEmpty) {
      val computeParams = new ComputeParams(searchIndexName, maxSplitsCount)
      hadoopConf.set(TableStoreInputFormat.COMPUTE_PARAMS, computeParams.serialize)
    } else {
      val computeParams = new ComputeParams(maxSplitsCount, splitSizeInMbs, computeMode)
      hadoopConf.set(TableStoreInputFormat.COMPUTE_PARAMS, computeParams.serialize)
    }
    val otsFilter = TableStoreFilter.buildFilters(filters, this)
    val otsRequiredColumns = requiredColumns.toList.asJava
    hadoopConf.set(TableStoreInputFormat.FILTER,
      new TableStoreFilterWritable(otsFilter, otsRequiredColumns).serialize)

    TableStore.setCredential(hadoopConf, new Credential(accessKeyId, accessKeySecret, null))
    val ep = new Endpoint(endpoint, instanceName)
    TableStore.setEndpoint(hadoopConf, ep)
    TableStoreInputFormat.addCriteria(hadoopConf, fetchCriteria())
    val rawRdd = sqlContext.sparkContext.newAPIHadoopRDD(
      hadoopConf,
      classOf[TableStoreInputFormat],
      classOf[PrimaryKeyWritable],
      classOf[RowWritable])

    val rdd = rawRdd.mapPartitions(it =>
      it.map { case (_, rw) =>
        val values = requiredColumns.map(fieldName => extractValue(rw.getRow, fieldName))
        Row.fromSeq(values)
      }
    )

    rdd
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val job = Job.getInstance(data.sparkSession.sparkContext.hadoopConfiguration)
    job.setOutputKeyClass(classOf[String])
    job.setOutputValueClass(classOf[String])
    job.setOutputFormatClass(classOf[TableStoreOutputFormat])
    val jobConfig = job.getConfiguration
    val tempDir = Utils.createTempDir()
    if (jobConfig.get("mapreduce.output.fileoutputformat.outputdir") == null) {
      jobConfig.set("mapreduce.output.fileoutputformat.outputdir",
        tempDir.getPath + "/outputDataset")
    }
    jobConfig.set(TableStoreOutputFormat.OUTPUT_TABLE, tbName)
    jobConfig.set(TableStore.CREDENTIAL,
      new Credential(accessKeyId, accessKeySecret, null).serialize())
    jobConfig.set(TableStore.ENDPOINT, new Endpoint(endpoint, instanceName).serialize())
    jobConfig.set(TableStoreOutputFormat.MAX_UPDATE_BATCH_SIZE, batchUpdateSize)

    // df.queryExecution.toRdd
    val rdd = data.rdd

    rdd.mapPartitions(it => {
      val tbMeta = fetchTableMeta()
      it.map(row => (null.asInstanceOf[Writable], convertToOtsRow(row, tbMeta)))
    }).saveAsNewAPIHadoopDataset(jobConfig)
  }

  private def convertToOtsRow(row: Row, tbMeta: TableMeta): BatchWriteWritable = {
    val batch = new BatchWriteWritable()
    val pkeyNames = new util.HashSet[String]()
    val pkeyCols = new util.ArrayList[PrimaryKeyColumn]()
    tbMeta.getPrimaryKeyList.asScala.foreach(otsSchema => {
      val name = otsSchema.getName
      pkeyNames.add(name)
      val pkeyCol = otsSchema.getType match {
        case PrimaryKeyType.INTEGER =>
          schema(name).dataType match {
            case LongType =>
              new PrimaryKeyColumn(name, PrimaryKeyValue.fromLong(row.getAs[Long](name)))
            case IntegerType =>
              new PrimaryKeyColumn(name, PrimaryKeyValue.fromLong(row.getAs[Int](name).toLong))
            case FloatType =>
              new PrimaryKeyColumn(name, PrimaryKeyValue.fromLong(row.getAs[Float](name).toLong))
            case DoubleType =>
              new PrimaryKeyColumn(name, PrimaryKeyValue.fromLong(row.getAs[Double](name).toLong))
            case ShortType =>
              new PrimaryKeyColumn(name, PrimaryKeyValue.fromLong(row.getAs[Short](name).toLong))
            case ByteType =>
              new PrimaryKeyColumn(name, PrimaryKeyValue.fromLong(row.getAs[Byte](name).toLong))
            case _ =>
              throw new SerDeException(s"Data type of column $name mismatch, " +
                s"expected: ${otsSchema.getType} real: ${schema(name).dataType}")
          }
        case PrimaryKeyType.STRING =>
          new PrimaryKeyColumn(name, PrimaryKeyValue.fromString(row.getAs[String](name)))
        case PrimaryKeyType.BINARY =>
          new PrimaryKeyColumn(name, PrimaryKeyValue.fromBinary(row.getAs[Array[Byte]](name)))
        case _ =>
          throw new SerDeException(s"unknown data type of primary key: ${otsSchema.getType}")
      }
      pkeyCols.add(pkeyCol)
    })

    val attrs = new util.ArrayList[Column]()
    schema.fieldNames.foreach(field => {
      if (!pkeyNames.contains(field)) {
        schema(field).dataType match {
          case LongType =>
            attrs.add(new Column(field, ColumnValue.fromLong(row.getAs[Long](field))))
          case IntegerType =>
            attrs.add(new Column(field, ColumnValue.fromLong(row.getAs[Int](field).toLong)))
          case FloatType =>
            attrs.add(new Column(field, ColumnValue.fromDouble(row.getAs[Float](field).toDouble)))
          case DoubleType =>
            attrs.add(new Column(field, ColumnValue.fromDouble(row.getAs[Double](field))))
          case ShortType =>
            attrs.add(new Column(field, ColumnValue.fromLong(row.getAs[Short](field).toLong)))
          case ByteType =>
            attrs.add(new Column(field, ColumnValue.fromLong(row.getAs[Byte](field).toLong)))
          case StringType =>
            attrs.add(new Column(field, ColumnValue.fromString(row.getAs[String](field))))
          case BinaryType =>
            attrs.add(new Column(field, ColumnValue.fromBinary(row.getAs[Array[Byte]](field))))
          case BooleanType =>
            attrs.add(new Column(field, ColumnValue.fromBoolean(row.getAs[Boolean](field))))
        }
      }
    })

    val putRow = new RowPutChange(tbName, new PrimaryKey(pkeyCols))
    putRow.addColumns(attrs)
    batch.addRowChange(putRow)
    batch
  }

  private def fetchCriteria(): RangeRowQueryCriteria = {
    val res = new RangeRowQueryCriteria(tbName)
    res.setMaxVersions(1)
    val lower = new util.ArrayList[PrimaryKeyColumn]()
    val upper = new util.ArrayList[PrimaryKeyColumn]()

    val meta = fetchTableMeta()
    for (schema <- meta.getPrimaryKeyList.asScala) {
      lower.add(new PrimaryKeyColumn(schema.getName, PrimaryKeyValue.INF_MIN))
      upper.add(new PrimaryKeyColumn(schema.getName, PrimaryKeyValue.INF_MAX))
    }
    res.setInclusiveStartPrimaryKey(new PrimaryKey(lower))
    res.setExclusiveEndPrimaryKey(new PrimaryKey(upper))
    res
  }

  private def fetchTableMeta(): TableMeta = {
    val ots = getOTSClient
    try {
      val resp = ots.describeTable(new DescribeTableRequest(tbName))
      resp.getTableMeta
    } finally {
      ots.shutdown()
    }
  }

  private def getOTSClient = {
    new SyncClient(endpoint, accessKeyId, accessKeySecret, instanceName)
  }

  private def extractValue(row: TSRow, fieldName: String): Any = {
    val isPrimaryKey = row.getPrimaryKey.contains(fieldName)
    val isPropertyKey = row.contains(fieldName)

    if (isPrimaryKey) {
      val pkColumn = row.getPrimaryKey.getPrimaryKeyColumn(fieldName)
      pkColumn.getValue.getType match {
        case PrimaryKeyType.INTEGER =>
          schema(pkColumn.getName).dataType match {
            case LongType =>
              pkColumn.getValue.asLong()
            case IntegerType =>
              pkColumn.getValue.asLong().toInt
            case FloatType =>
              pkColumn.getValue.asLong().toFloat
            case DoubleType =>
              pkColumn.getValue.asLong().toDouble
            case ShortType =>
              pkColumn.getValue.asLong().toInt
            case ByteType =>
              pkColumn.getValue.asLong().toByte
            case _ =>
              throw new SerDeException(s"data type mismatch, " +
                s"expected: ${schema(pkColumn.getName).dataType} " +
                s"real: ${pkColumn.getValue.getType}")
          }
        case PrimaryKeyType.STRING =>
          pkColumn.getValue.asString()
        case PrimaryKeyType.BINARY =>
          pkColumn.getValue.asBinary()
        case _ =>
          throw new SerDeException(s"unknown data type of primary " +
            s"key: ${pkColumn.getValue.getType}")
      }
    } else if (isPropertyKey) {
      val col = row.getLatestColumn(fieldName)
      col.getValue.getType match {
        case ColumnType.INTEGER =>
          val value = col.getValue.asLong()
          schema(col.getName).dataType match {
            case LongType =>
              value.toLong
            case IntegerType =>
              value.toInt
            case FloatType =>
              value.toFloat
            case DoubleType =>
              value.toDouble
            case ShortType =>
              value.toInt
            case ByteType =>
              value.toByte
            case _ =>
              throw new SerDeException(s"data type mismatch, " +
                s"expected: ${schema(col.getName).dataType} real: ${col.getValue.getType}")
          }
        case ColumnType.DOUBLE =>
          col.getValue.asDouble()
        case ColumnType.STRING =>
          col.getValue.asString()
        case ColumnType.BOOLEAN =>
          col.getValue.asBoolean()
        case ColumnType.BINARY =>
          col.getValue.asBinary()
        case _ =>
          throw new SerDeException(s"unknown data type of primary key: ${col.getValue.getType}")
      }
    } else {
      logWarning(s"unknown field name: $fieldName")
      null
    }
  }

  // TODO 有geo列并且sparksql全是and的时候,geo列和大多数filter肯定会被handled,只有一些数据量很大filter,交由spark过滤
  //有geo列并且sparksql里面有or的时候,算子必须全部推到ots-sdk,并且unhandleFilters必须返回空数组(默认所有filter都转化成了query,
  // 并交由ots-sdk执行,不给spark过滤geo列的权利)

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    val configuration = new ClientConfiguration()
    var otsClient = new SyncClient(endpoint, accessKeyId, accessKeySecret, instanceName, configuration)
    if (searchIndexName.isEmpty) {
      filters
      //Or的情况 返回空数组
    } else {
      var unhandledSparkFilters = new ArrayBuffer[Filter]()
      var otsFilterPushed = TableStoreFilter.buildFilters(filters, this)
      TablestoreSplit.beforeGetUnhandledOtsFilter(otsClient, otsFilterPushed, tbName, searchIndexName);
      var filterOtsUnhandled: OTSFilter = TablestoreSplit.getUnhandledOtsFilter(otsClient, otsFilterPushed, tbName, searchIndexName)
      if (filterOtsUnhandled == null) {
        return unhandledSparkFilters.toArray
      } else if (!filterOtsUnhandled.isNested) {
        val filterSpark = otsfilterToSparkFilterArray(filterOtsUnhandled)
        //      val filterSpark = otsfilterToSparkFilterArray(null)
        unhandledSparkFilters += filterSpark
      } else {
        //因为全是and下的filter所以不需要递归
        val subFilters = filterOtsUnhandled.getSubFilters().asScala
        for (filterOtsUnhandled2 <- subFilters) {
          //          var filterOtsUnhandled2 = subFilters.get(i)
          var filterSpark = otsfilterToSparkFilterArray(filterOtsUnhandled2)
          unhandledSparkFilters += filterSpark
        }
      }
      return unhandledSparkFilters.toArray
    }
    new Array[Filter](0)
  }

  private def otsfilterToSparkFilterArray(filterOts: OTSFilter): Filter = {
    //        return new GreaterThanOrEqual("val_long1", 3697900)
    if (filterOts.getCompareOperator == OTSFilter.CompareOperator.EQUAL) {
      return new EqualTo(filterOts.getColumnName, filterOts.getColumnValue.getValue)
    } else if (filterOts.getCompareOperator == OTSFilter.CompareOperator.IS_NULL) {
      return new IsNotNull(filterOts.getColumnName)
      //startWith类型
    } else if (filterOts.getCompareOperator == OTSFilter.CompareOperator.START_WITH) {
      return new StringStartsWith(filterOts.getColumnName, filterOts.getColumnValue.asString())
    } else if (filterOts.getCompareOperator == OTSFilter.CompareOperator.GREATER_THAN) {
      return new GreaterThan(filterOts.getColumnName, filterOts.getColumnValue.getValue)
    } else if (filterOts.getCompareOperator == OTSFilter.CompareOperator.GREATER_EQUAL) {
      return new GreaterThanOrEqual(filterOts.getColumnName, filterOts.getColumnValue.getValue)
    } else if (filterOts.getCompareOperator == OTSFilter.CompareOperator.LESS_THAN) {
      return new LessThan(filterOts.getColumnName, filterOts.getColumnValue.getValue)
    } else if (filterOts.getCompareOperator == OTSFilter.CompareOperator.LESS_EQUAL) {
      return new LessThanOrEqual(filterOts.getColumnName, filterOts.getColumnValue.getValue)
    }
    null
    //    } else if (filterOts.getCompareOperator eq OTSFilter.CompareOperator.IN) {
    //      return new In(filterOts.getColumnName, filterOts.getColumnValuesForInOperator.toArray())
    //    else if (filter.getCompareOperator eq Filter.CompareOperator.NOT_EQUAL) { //notEqual
    //      return new
    //    }

    //    null
  }

}
