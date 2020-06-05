package com.yiyuan.metaRetrieve

import io.minio.MinioClient
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

class getData {
  def getSparkData(sparkSess:SparkSession,readPath:String):sql.DataFrame = {
    val minioObj = new MinioClient(
      sparkSess.sparkContext.getConf.get("spark.hadoop.fs.s3a.endpoint"),
      sparkSess.sparkContext.getConf.get("spark.hadoop.fs.s3a.access.key"),
      sparkSess.sparkContext.getConf.get("spark.hadoop.fs.s3a.secret.key")
    )

    val splitone = readPath.split("://")
    val splittwo = splitone(1).split("/")
    val objectData = minioObj.statObject(splittwo(0),splitone(1).split(splittwo(0)+"/")(1)).httpHeaders().get("X-Amz-Meta-User.schemadata")
    val schemaData = objectData.get(0)

    val schemadataSplit = schemaData.split(" ")

    val formatType = schemadataSplit.last.toLowerCase match {
      case "csv" => "csv"
      case "jsontrue" => "json"
      case "jsonfalse" => "json"
      case "parquet" => "parquet"
      case "avro" => "avro"
      case "orc" => "orc"
      case _ => {
        println(s"File type ${schemadataSplit.last} not supported, supported file types are csv, jsontrue/jsonfalse, parquet, text, avro and orc.")
        sys.exit(0)
      }
    }

    var multilineOption = "true"
    if(schemadataSplit.last.toLowerCase.contains("json"))
      multilineOption = schemadataSplit.last.drop(4)

    val typeData = schemadataSplit.takeWhile(_ != schemadataSplit.last)

    if(formatType.equals("parquet") || formatType.equals("avro") || formatType.equals("orc")){
      println("Data will be read as it is as attaching schema is not possible.")
      val directRead = sparkSess.read.format(formatType).load(readPath)
      directRead
    }
    else if(!typeData(0).equalsIgnoreCase("readFromFile")) {
      val mappedTypeData = typeData.map(base => {
        val datasplit = base.split("/")
        (datasplit(0), datasplit(1))
      })

      var arrayForType = ArrayBuffer[String]()
      mappedTypeData.map(line => {
        val noOfType = line._1.toInt
        val typeName = line._2
        for (x <- 1 to noOfType) {
          arrayForType += typeName
        }
      })

      //Add more here to accommodate for more types
      val arrayToList = arrayForType.toList.map(_ match {
        case "Timestamp" => TimestampType
        case "String" => StringType
        case "Integer" => IntegerType
        case "Double" => DoubleType
        case _ => StringType
      })

      val typeListIterator = arrayToList.iterator

      val dfForHeader = sparkSess.read.format(formatType).options(Map("header" -> "true", "multiline" -> multilineOption)).load(readPath)
      var HeaderString = ""
      dfForHeader.schema.fieldNames.foreach(HeaderString += _ + ",")
      HeaderString.dropRight(1)
      val headersplit = HeaderString.split(",") //get all header names for schema

      val newSchema = StructType(headersplit.map(StructField(_, typeListIterator.next(), true))) //nullable=true

      //Show schema of data when read directly
      val dfWithNoSchema = sparkSess.read.format(formatType).options(Map("header" -> "true", "multiline" -> multilineOption)).load(readPath)
      println("Schema from directly reading")
      dfWithNoSchema.printSchema()

      val dfWithSchema = sparkSess.read.format(formatType).schema(newSchema)
        .options(Map("header" -> "true", "multiline" -> multilineOption))
        .load(readPath)
      dfWithSchema
    }
    else {
      val objectData2 = minioObj.statObject(splittwo(0),splitone(1).split(splittwo(0)+"/")(1)).httpHeaders().get("X-Amz-Meta-User.pathtoschema")
      val pathToSchema = objectData2.get(0)

      val fileWithSchema = sparkSess.read.textFile(pathToSchema).collect()(0)

      val dataWithStringSchema = sparkSess.read.schema(DataType.fromJson(fileWithSchema).asInstanceOf[StructType])
        .format(formatType)
        .options(Map("header"->"true","multiline"->multilineOption))
        .load(readPath)

      dataWithStringSchema
    }
  }
}
