import java.io.{File, PrintWriter}
import org.apache.spark.sql.SparkSession

//This will create a file containing the schema of the data that is read.

//There is a library dependency conflict if this is ran here, use spark-submit to run it.
//Specify "--class SchemaFilefromData" when submitting spark-job.

object SchemaFilefromData {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("schemafromdata").getOrCreate()

    val pathToFile = ""
    val fileExtension = ""                    //csv json parquet orc avro
    val fileName = ""
    val dataSch = spark.read
      .format(fileExtension)
      .option("header","true")          //for csv
      .load(pathToFile)

    val writer = new PrintWriter(new File(fileName))
    writer.write(dataSch.schema.json)
    writer.close()
  }
}
