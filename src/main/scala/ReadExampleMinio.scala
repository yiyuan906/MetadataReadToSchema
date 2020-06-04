import com.yiyuan.metaRetrieve.getData
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadExampleMinio {
  def main(args:Array[String]): Unit = {
    if(args.length!=1){
      println("Please add on a path as argument to read from.")
      sys.exit(1)
    }

    val endpoint = "http://127.0.0.1:9000"
    val access_key = "minio"
    val secret_key = "minio123"

    val conf = new SparkConf()
      .set("spark.hadoop.fs.s3a.endpoint", endpoint)
      .set("spark.hadoop.fs.s3a.access.key", access_key)
      .set("spark.hadoop.fs.s3a.secret.key", secret_key)
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    val Spark = SparkSession.builder()
      .config(conf)
      .master("local[*]")
      .appName("Metadata read to schema example")
      .getOrCreate()

    val dfFunction = new getData().getSparkData(Spark, args(0))
    println("Schema after reading metadata")
    dfFunction.printSchema()
    dfFunction.show(20)
  }
}
