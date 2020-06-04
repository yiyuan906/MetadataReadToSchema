import java.io.{File, PrintWriter}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import scala.collection.mutable.ArrayBuffer

//This can create a schema which is structured according to the types and header names given.
//It does not support nested structtypes.

object SchemaFileFromStruct {
  def main(args:Array[String]): Unit = {
    val schemaData = ""                           //enter datatype for struct like "1/Timestamp 1/Integer 8/String 1/Double 2/String"
    val headerList = ""                           //enter header names for data by order of the datatype
    val filename = ""                             //file name for created file
    val schemadataSplit = schemaData.split(" ")

    val typeData = schemadataSplit

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

    val arrayToList = arrayForType.toList.map(_ match {           //add more types here to accommodate for new types
      case "Timestamp" => TimestampType
      case "String" => StringType
      case "Integer" => IntegerType
      case "Double" => DoubleType
      case _ => StringType
    })

    val typeListIterator = arrayToList.iterator
    val headersplit = headerList.split(" ")
    val structSchema = StructType(headersplit.map(StructField(_, typeListIterator.next(), true)))
    val writer = new PrintWriter(new File(filename))
    writer.write(structSchema.json)
    writer.close()
  }
}
