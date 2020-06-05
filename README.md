# MetadataReadToSchema

This example shows a way to make use of the metadata set by users to get a designated schema when reading data in spark from MinIO.
At its core, the function still uses SparkSession to read the data, just that the process of creating a StructType for the schema of the
data will be done using the metadata provided. Due to the way the function is implemented, it does not allow for nested structures, 
but it allows for reading of StructType which has been converted to json which is stored in the form of a textfile. 
This function will only work for reading of csv, json and avro as their schemas can be specified. 
For parquet and orc file formats, it will just be treated as a normal read using SparkSession, but it will still require the filetype
for its metadata if the file is to be read through the function.

## Description of the function

There are two arguments which are required to be passed into the function, SparkSession and the path to the file. 
The SparkSession passed should include the necessary details to connect to MinIO.
The path only caters to files inside MinIO and is specified using `s3a://bucket-name/path/to/file`.
Metadata is required for the file to be read in this function.

The first method: The current implementation makes use of the metadata to map the StructType and get the file extension. For example,
`1/String 1/Integer 8/String csv` would give a StructType that contain Structfields of those types in that order and note that the file is 
of csv extension. The names of those Structfields will be taken from the headers of the file directly through a read of the original not 
inferred schema fieldnames. The StructType created is then used as the schema when reading from that file, this dataframe is then returned
from the function. (applicable for csv and json)

The second method: This way works by making use of 2 lines of metadata, the one which is used in the first method and another metadata
which gives a path. The content of the first metadata would determine if it is method one or two. In this method, the path given is expected to 
lead to a textfile in MinIO which contains the StructType in json, after reading it, it will be converted back to the StructType to be
used to read the data and the dataframe is then returned from the function. For example, metadata1= `readfromfile json` and 
metadata2 = `s3a://bucket-name/schemafileinjsonstring.txt` would note that the file is of json extension and will use the schema which is
stored in `schemafileinjsonstring.txt` to do the reading. (applicable for csv, json and avro)

metadata name in the function:
- User.schemadata              (first line)
- User.pathtoschema            (second line)

Reading of avro file type is not implemented yet, so it is the same as parquet and orc at the moment. What is required for it is a avro
schema that is stored inside a textfile, so it will use the second method. The first method creates a StructType containing Structfields
for its data and that is not applicable as it is not accepted as a avro schema.

## Setup required

- VM uses ubuntu-18.04.4
- IntelliJ IDEA used as the IDE
- java -version `openjdk version "1.8.0_252"`
- sbt used to compile file

### MinIO server setup
Install MinIO Server from [here](https://docs.min.io/docs/minio-quickstart-guide).

The endpoint, access key and secret key used for this example is https://127.0.0.1:9000, minio and minio123 respectively.
To setup the access key and secret key when starting up the server, do `MINIO_ACCESS_KEY=minio MINIO_SECRET_KEY=minio123 ./minio server /path/to/local/miniostorage`

This settles the setup of MinIO. To check, go to https://127.0.0.1:9000 and enter the access key and secret key to enter.

Also download MinIO client from [here](https://docs.min.io/docs/minio-client-quickstart-guide)

Under the cmd which is running the MinIO server, the line under `Command-line Access` needs to be ran to let the MinIO client identify the MinIO server.
It will be something like `mc config host add myminio https://10.0.2.15:9000 minio minio123`, this MinIO server is then identified as `myminio`.

### Spark setup
Download Apache Spark version `spark-2.4.5-bin-hadoop2.7.tgz` from [here](https://spark.apache.org/downloads.html).

The dependencies required for Spark and MinIO connection are:
  - [`Hadoop AWS 2.7.3`](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/2.7.3)
  - [`HttpClient 4.5.3`](https://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient/4.5.3)
  - [`Joda Time 2.9.9`](https://mvnrepository.com/artifact/joda-time/joda-time/2.9.9)
  - [`AWS SDK For Java Core 1.11.712`](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-core/1.11.712)
  - [`AWS SDK For Java 1.11.712`](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk/1.11.712)
  - [`AWS Java SDK For AWS KMS 1.11.712`](http://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-kms/1.11.712)
  - [`AWS SDK For Java 1.7.4`](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk/1.7.4)
  - [`Spark Avro 2.4.5`](https://mvnrepository.com/artifact/org.apache.spark/spark-avro_2.11/2.4.5)
  
The dependencies required for the program's MinIO are:
  - [`Simple XML 2.7.1`](https://mvnrepository.com/artifact/org.simpleframework/simple-xml/2.7.1)
  - [`Minio 7.0.2`](https://mvnrepository.com/artifact/io.minio/minio/7.0.2)
  - [`Guava: Google Core Libraries For Java 28.2-jre`](https://mvnrepository.com/artifact/com.google.guava/guava/28.2-jre)
  
Extract the `spark-2.4.5-bin-hadoop2.7.tgz` tar ball in the directory where Spark is suppose to be installed. 
Afterwards, move all the dependency jar files downloaded previously into `/path/to/spark-2.4.5-bin-hadoop2.7/jars` directory.

### Setting of metadata
The metadata can be set by using `setfattr -n key1 -v value1 filename`.
The MinIO client is used to copy the file over to the server, directly uploading it would result in the uploaded file not having the
set metadata. `mc cp filename myminio/bucket-name/filename`

Another alternative is to copy the file over and write the metadata by using `mc cp --attr key1=value1 filename myminio/bucket-name/filename`.

The metadata can be checked my running `mc stat myminio/bucket-name/filename`

## Running the example program

Ensure file contains appropriate metadata before running the example program.

`ReadExampleMinio` is the example which uses the function. The configuration of minio in the example is same as stated in the MinIO server 
setup section. There is no error handling in this example.

The example is expected to print two schemas and show 20 rows if it is of json or csv type when the first method is used, the first schema
showing the default schema and the second schema showing the schema that is made based on the given metadata. 
The metadata required will look something like `X-Amz-Meta-User.schemadata="1/Timestamp 1/Integer 8/String 1/Double 2/String csv"`.

The second method will print the schema as specified from the file given from the path in the metadata and 20 rows.
The metadata required will look something like `X-Amz-Meta-User.schemadata=readFromFile jsonfalse` and 
`X-Amz-Meta-User.pathtoschema="s3a://bucket-name/path/to/schemafile.txt"`

If it is a avro, parquet or orc file, the metadata just has to note that it is of that type. 
The function would then be equivalent to just doing a normal read with that file format and the schema is given from the file itself.
Eg. `X-Amz-Meta-User.schemadata="avro"`

1. Build by doing `sbt package`
2. Run it by using `spark-submit --class ReadExampleMinio path/to/generated/jar s3a://bucket-name/path/to/file`

The other two scala files can help setup schema into files for the second method.

