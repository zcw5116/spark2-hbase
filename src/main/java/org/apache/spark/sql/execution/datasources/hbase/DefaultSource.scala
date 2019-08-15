/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.hbase

import java.sql.Timestamp
import com.google.gson.JsonParser
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.util.{SerializableConfiguration, Utils}
import org.joda.time.DateTime

import scala.collection.mutable

/**
  * Created by zhoucw on 2019-08-12 21:24.
  */
class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    HBaseRelation(parameters, None)(sqlContext)

  override def shortName(): String = "hbase"

  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    parameters: Map[String, String],
    data: DataFrame): BaseRelation = {
    val relation = InsertHBaseRelation(data, parameters, mode)(sqlContext)
    relation.createTable(parameters.getOrElse("numReg", "3").toInt)
    relation.insert(data, false)
    relation
  }
}

case class InsertHBaseRelation(
  dataFrame: DataFrame,
  parameters: Map[String, String],
  mode: SaveMode
)(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with InsertableRelation
  with Logging {

  private val wrappedConf = {
    new SerializableConfiguration(HBaseConfBuilder.build(dataFrame.sparkSession, parameters))
  }

  def hbaseConf: Configuration = wrappedConf.value

  val jHtable = parameters("htable")
  val catalog = parameters("catalog")

  val errorIfFieldUnmapping = parameters.contains("error_if_field_unmapped") match {
    case false => true
    case true => {
      parameters("error_if_field_unmapped").toLowerCase() match {
        case "true" => true
        case _ => false
      }
    }
  }



  val jNameSpace = parameters.contains("namespace") match {
    case true => parameters("namespace")
    case false => "default"
  }

  var jColumns: mutable.Map[String, mutable.Map[String, String]] = mutable.Map[String, mutable.Map[String, String]]()
  var jRowkey: String = null

  val family_unassigned = "cf_unassigned"

  loadCatalog()

  def loadCatalog(): Unit = {

    val jObj = new JsonParser().parse(catalog).getAsJsonObject
     jRowkey = jObj.get("rowkey").getAsString
    val columnIterator = jObj.get("columns").getAsJsonObject.entrySet().iterator()
    //val jColumnMap : mutable.Map[String, mutable.Map[String, String]] = jColumnMap : mutable.Map[String, mutable.Map[String, String]]()
    while(columnIterator.hasNext){
      val ele = columnIterator.next()
      val fieldName = ele.getKey
      val hbaseColumnIter = ele.getValue.getAsJsonObject.entrySet().iterator()
      val hbaseColumnMap:mutable.Map[String, String] = mutable.Map[String, String]()

      while(hbaseColumnIter.hasNext){
        val hbaseColumnE = hbaseColumnIter.next()
        hbaseColumnMap.put(hbaseColumnE.getKey, hbaseColumnE.getValue.getAsString)
      }
      jColumns.put(fieldName, hbaseColumnMap)
    }
  }

  def createTable(numReg: Int) {
    val family = ""

    val familySet = jColumns
      .mapValues(v => {
        v("cf")
       // v.asInstanceOf[Map[String, String]]("cf").toString
      })
      .values
      .toSet

    val tName = TableName.valueOf(jHtable)

    val connection = ConnectionFactory.createConnection(hbaseConf)
    // Initialize hBase table if necessary
    val admin = connection.getAdmin
    if (numReg > 3) {
      if (!admin.isTableAvailable(tName)) {
        val tableDesc = new HTableDescriptor(tName)

        for (f <- familySet) {
          val cf = new HColumnDescriptor(Bytes.toBytes(f))
          tableDesc.addFamily(cf)
        }

        val startKey = Bytes.toBytes("aaaaaaa")
        val endKey = Bytes.toBytes("zzzzzzz")
        val splitKeys = Bytes.split(startKey, endKey, numReg - 3)
        admin.createTable(tableDesc, splitKeys)
        val r = connection.getRegionLocator(TableName.valueOf(jHtable)).getAllRegionLocations
        while (r == null || r.size() == 0) {
          logDebug(s"region not allocated")
          Thread.sleep(1000)
        }
        logInfo(s"region allocated $r")

      }
    } else {
      if (!admin.isTableAvailable(tName)) {
        val tableDesc = new HTableDescriptor(tName)

        for (f <- familySet) {
          val cf = new HColumnDescriptor(Bytes.toBytes(f))
          tableDesc.addFamily(cf)
        }
        admin.createTable(tableDesc)
      }
    }
    admin.close()
    connection.close()
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {

    val fields = data.schema.toArray
    val rowkeyIndex = fields.zipWithIndex.filter(f => f._1.name == jRowkey).head._2
    var otherFields = fields.zipWithIndex.filter(f => f._1.name != jRowkey)

    val rdd = data.rdd //df.queryExecution.toRdd

    val tsSuffix = parameters.get("tsSuffix") match {
      case Some(tsSuffix) => otherFields = otherFields.filter(f => !f._1.name.endsWith(tsSuffix)); tsSuffix
      case _ => ""
    }

    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, jHtable)
    val job = Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])
    val jobConfig = job.getConfiguration
    val tempDir = Utils.createTempDir()
    if (jobConfig.get("mapreduce.output.fileoutputformat.outputdir") == null) {
      jobConfig.set("mapreduce.output.fileoutputformat.outputdir", tempDir.getPath + "/outputDataset")
    }

    def convertToPut(row: Row) = {

      val put = new Put(Bytes.toBytes(row.getString(rowkeyIndex)))
      otherFields.foreach { field =>
        if (row.get(field._2) != null) {

          val st = field._1
          val fieldName = st.name
          var cf = "cf_unassigned"
          var columnName = fieldName
          var datatype = "string"
          val fieldIndex = field._2

          if (jColumns.contains(fieldName)) {
            val columnInfo = jColumns(fieldName)
            cf = columnInfo("cf")
            columnName = columnInfo("col")

            if(columnInfo.contains("type")){
              datatype = columnInfo("type")
            }

            val cvalue = st.dataType match {
              case StringType => row.getString(fieldIndex)
              case FloatType => row.getFloat(fieldIndex)
              case DoubleType => row.getDouble(fieldIndex)
              case LongType => row.getLong(fieldIndex)
              case IntegerType => row.getInt(fieldIndex)
              case BooleanType => row.getBoolean(fieldIndex)
              case DateType => new DateTime(row.getDate(fieldIndex)).getMillis
              case TimestampType => new DateTime(row.getTimestamp(fieldIndex)).getMillis
              case ShortType => row.getShort(fieldIndex)
              case BinaryType => row.getAs[Array[Byte]](fieldIndex)
              case ByteType => row.getAs[Byte](fieldIndex)
              case _ => row.getString(fieldIndex)
            }

            val tsField = field._1.name + tsSuffix
            if (StringUtils.isNoneBlank(tsSuffix) && row.schema.fieldNames.contains(tsField)) {
              put.addColumn(
                Bytes.toBytes(cf),
                Bytes.toBytes(columnName),
                row.getAs[Long](tsField),
                Bytes.toBytes(cvalue.toString))
            } else {
              put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columnName), Bytes.toBytes(cvalue.toString))
            }
          } else {
            if(errorIfFieldUnmapping){
              throw new Exception(s"field ${fieldName} is not assigned to any column family.")
            }
          }
        }
      }
      (new ImmutableBytesWritable, put)
    }

    rdd.map(convertToPut).filter(!_._2.isEmpty).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  override def schema: StructType = dataFrame.schema
}

case class HBaseRelation(
  parameters: Map[String, String],
  userSpecifiedschema: Option[StructType]
)(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with TableScan
  with Logging {

  val catalog = parameters("catalog")
  val inputTableName = parameters("htable")
  val jNameSpace = parameters.contains("namespace") match {
    case true => parameters("namespace")
    case false => "default"
  }

  var unassinedField:String = null
  if(parameters.contains("columns_unmapped_to_field") &&  parameters("columns_unmapped_to_field").trim.length>0)
    {
      unassinedField = parameters("columns_unmapped_to_field")
    }


  var jColumns: mutable.Map[String, mutable.Map[String, String]] = mutable.Map[String, mutable.Map[String, String]]()
  var jRowkey: String = null

  loadCatalog()

  def loadCatalog(): Unit = {

    val jObj = new JsonParser().parse(catalog).getAsJsonObject
    jRowkey = jObj.get("rowkey").getAsString
    val columnIterator = jObj.get("columns").getAsJsonObject.entrySet().iterator()
    //val jColumnMap : mutable.Map[String, mutable.Map[String, String]] = jColumnMap : mutable.Map[String, mutable.Map[String, String]]()
    while(columnIterator.hasNext){
      val ele = columnIterator.next()
      val fieldName = ele.getKey
      val hbaseColumnIter = ele.getValue.getAsJsonObject.entrySet().iterator()
      val hbaseColumnMap:mutable.Map[String, String] = mutable.Map[String, String]()

      while(hbaseColumnIter.hasNext){
        val hbaseColumnE = hbaseColumnIter.next()
        hbaseColumnMap.put(hbaseColumnE.getKey, hbaseColumnE.getValue.getAsString)
      }
      jColumns.put(fieldName, hbaseColumnMap)
    }
  }

  private val wrappedConf = {
    val hc = HBaseConfBuilder.build(sqlContext.sparkSession, parameters)
    hc.set(TableInputFormat.INPUT_TABLE, inputTableName)
    new SerializableConfiguration(hc)
  }

  def hbaseConf: Configuration = wrappedConf.value

  def buildScan(): RDD[Row] = {

    val fields = schema.toArray
    // val rowkeyIndex = fields.zipWithIndex.filter(f => f._1.name == jRowkey).head._2
    // var otherFields = fields.zipWithIndex.filter(f => f._1.name != jRowkey)
    val fieldsWithIndex = fields.zipWithIndex

    val columnInfo = fieldsWithIndex
      .filter(!_._1.name.equals(unassinedField))
      .map(x => {
        val field = x._1.name
        val index = x._2
        if (field.equals(jRowkey)) {
          (jRowkey, index)
        } else {
          val hColumn = jColumns(field)
          (hColumn("cf") + ":" + hColumn("col"), index)
        }
      })
      .toMap

    val tsSuffix = parameters.getOrElse("tsSuffix", "_ts_zhoucw")
    val family = parameters.getOrElse("family", "")
    val columnSize = columnInfo.size
    val schemaArr = schema.toArray
    val hBaseRDD = sqlContext.sparkContext
      .newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .map { line =>
        val rowKey = Bytes.toString(line._2.getRow)
        val arr = new Array[Any](columnSize)
        arr(columnInfo(jRowkey)) = rowKey

        import _root_.net.liftweb.{json => SJSon}
        implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
        val unAssignedCols = new mutable.HashMap[String, Any]()

        val content = line._2
          .rawCells()
          .foreach(cell => {

            val f = new String(CellUtil.cloneFamily(cell))

            val columnName = new String(CellUtil.cloneQualifier(cell))
            val fullColumnName = f + ":" + columnName
            //val hbaseDatatype = jColumns(columnName).asInstanceOf[Map[String, String]]
            columnInfo.contains(fullColumnName) match {
              case false => {
                unAssignedCols.+=((fullColumnName, Bytes.toString(CellUtil.cloneValue(cell))))
              }
              case true => {
                val fieldIndex = columnInfo(fullColumnName)
                val hbaseColumnInfo = jColumns(schemaArr(fieldIndex).name)
                val hbaseDatatype = hbaseColumnInfo.contains("type") match {
                  case true => hbaseColumnInfo("type").toLowerCase()
                  case false => "string"
                }
                val value = hbaseDatatype match {
                  case "long" => Bytes.toLong(CellUtil.cloneValue(cell))
                  case "float" => Bytes.toFloat(CellUtil.cloneValue(cell))
                  case "double" => Bytes.toDouble(CellUtil.cloneValue(cell))
                  case "integer" => Bytes.toInt(CellUtil.cloneValue(cell))
                  case "boolean" => Bytes.toBoolean(CellUtil.cloneValue(cell))
                  case "binary" => CellUtil.cloneValue(cell)
                  case "timestamp" => new Timestamp(Bytes.toLong(CellUtil.cloneValue(cell)))
                  case "date" => new java.sql.Date(Bytes.toLong(CellUtil.cloneValue(cell)))
                  case "short" => Bytes.toShort(CellUtil.cloneValue(cell))
                  case "byte" => CellUtil.cloneValue(cell).head
                  case _ => Bytes.toString(CellUtil.cloneValue(cell))
                }
                arr(fieldIndex) = value
              }
            }
          })

        var row = Row.fromSeq(arr)
        if (unassinedField!=null) {
          val unAssignedColsStr = SJSon.Serialization.write(unAssignedCols.toMap)
          row = Row.merge(Row.fromSeq(arr), Row(unAssignedColsStr))
        }
        row
      }
    hBaseRDD
  }

  override def schema: StructType = {

    var schema = new StructType()

    schema = schema.add(jRowkey, "string")
    for (jColum <- jColumns) {
      val fieldName = jColum._1
      val hColumn = jColum._2
      val datatype = hColumn.contains("type") match {
        case true => hColumn("type")
        case false => "string"
      }
      schema = schema.add(fieldName, datatype)
    }
    if (unassinedField!=null) {
      schema = schema.add(unassinedField, "string")
    }

    schema
  }
}

object HBaseParams {
  val ROW_KEY = "rowkey"
}
