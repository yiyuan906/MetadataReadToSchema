name := "MetadataReadToSchema"

version := "0.1"

scalaVersion := "2.11.12"

//lazy val shaded = (project in file(".")).settings(commonSettings)

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "io.minio" % "minio" % "7.0.2"
//libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.712"
libraryDependencies += "org.apache.spark" %% "spark-avro" % "2.4.5"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.7.4"

