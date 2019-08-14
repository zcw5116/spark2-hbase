# document

## Write to Hbase

- Generate DataFrame
```
val spark = SparkSession.builder().master("local[*]").getOrCreate()
import spark.implicits._
val list = List(("a1", "aa1", 123, "hehe"), ("b1", "bb1", 456, "hehe"))
val df = spark.sparkContext.makeRDD(list).toDF("myrowkey", "mycol0", "mycol1", "mycol3")
  
```
- Define catalog

```
val catalog = s"""{
                     |"rowkey":"myrowkey",
                     |"columns":{
                     |"mycol0":{"cf":"cf1", "col":"c1", "type":"string"},
                     |"mycol1":{"cf":"cf2", "col":"c2", "type":"string"},
                     |"mycol2":{"cf":"cf3", "col":"c3", "type":"string"}
                     |}
                     |}""".stripMargin
```
rowkey is mapping to dataframe field 'myrowkey'
columns is defined for mapping dataframe fields to hbase columns 


- Write DataFrame to Hbase

```
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
```
If fields of dataframe are not appeared in catalog, an exception will throw. 
Option 'error_if_field_unmapped' can be setted to false to avoid exception. 

## Read from Hbase

- Define catalog

rowkey is mapping to dataframe field 'myrowkey'
columns is defined for mapping dataframe fields to hbase columns 


```
val catalog = s"""{
                     |"rowkey":"myrowkey",
                     |"columns":{
                     |"mycol0":{"cf":"cf1", "col":"c1", "type":"string"},
                     |"mycol1":{"cf":"cf2", "col":"c2", "type":"string"},
                     |"mycol2":{"cf":"cf3", "col":"c3", "type":"string"}
                     |}
                     |}""".stripMargin
```

- Read from Hbase as DataFrame

```
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
```

If hbase columns are not mapped to DataFrame Fields, these columns are collected to one column defined by option 'columns_unmapped_to_field'.

If option 'columns_unmapped_to_field' is not assigned, then these columns are dropped.