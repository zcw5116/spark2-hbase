package hbase.app

import org.apache.spark.sql.SparkSession

/**
  * Created on 2019-08-09 10:30.
  */
object TestSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    val list = List(("a1", "aa1", 123, "hehe"), ("b1", "bb1", 456, "hehe"))
    val df = spark.sparkContext.makeRDD(list).toDF("myrowkey", "mycol0", "mycol1", "mycol3")
   // df.show()
    df.printSchema()
    io.netty.buffer.PooledByteBufAllocator.defaultNumHeapArena()
    val sc = spark.sparkContext
    val catalog = s"""{
                     |"rowkey":"myrowkey",
                     |"columns":{
                     |"mycol0":{"cf":"cf1", "col":"c1", "type":"string"},
                     |"mycol1":{"cf":"cf2", "col":"c2", "type":"string"},
                     |"mycol2":{"cf":"cf3", "col":"c3", "type":"string"}
                     |}
                     |}""".stripMargin

    val tableName = "mytest1345"

     df.write
      .options(Map(
        "table_name" -> tableName,
        "error_if_field_unmapped" -> "false",
        "catalog" -> catalog,
        "zk" -> "localhost:2181"
      ))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()



    val df1 = spark.read
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .options(
      Map(
        "columns_unmapped_to_field"->"myunn",
        "table_name" -> tableName,
        "catalog" -> catalog
      )
    ).load()

    df1.show(false)



  }
}
