package hbase.app

import com.google.gson.JsonParser

/**
  * Created on 2019-08-14 15:26.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val catalog = s"""{
                     |"rowkey":"myrowkey",
                     |"columns":{
                     |"mycol0":{"cf":"cf1", "col":"c1", "type":"string"},
                     |"mycol1":{"cf":"cf2", "col":"c2", "type":"string"},
                     |"mycol2":{"cf":"cf3", "col":"c3", "type":"string"}
                     |}
                     |}""".stripMargin

     val json = new JsonParser().parse(catalog)
   println(json)
    import scala.collection.JavaConversions._
    val colums = json.getAsJsonObject.get("columns")

    val has = json.getAsJsonObject.has("columns")
  }
}
