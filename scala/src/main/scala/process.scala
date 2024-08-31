import org.apache.hadoop.hdfs._
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType}
import io.delta.tables
import org.apache.hadoop.fs.{FileUtil,Path,FileSystem,LocalFileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j._
import org.apache.spark.TaskContext
import sys.process._


object data_processor extends App {

    def init_spark() : SparkSession = {
       SparkSession
          .builder()
          .master("local[*]")
          .appName("DATA-PROCESSOR")
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()
    }

    val spark = init_spark()
    import spark.implicits._

    val sc = spark.sparkContext

    val src_file_path = new Path("file:///C:/Users/Prajwal/nse_project/main/raw/")

    val hdfs_destination_path = new Path("hdfs://localhost:9000/raw/")

    // Hadoop configuration to use LocalFileSystem explicitly
    val conf = new Configuration()

  // Set to your HDFS namenode address
    conf.set("fs.defaultFS", "hdfs://localhost:9000") 

   //  conf.set("hadoop.native.lib", "false") 
   //  conf.setBoolean("hadoop.native.lib.disable", true)
   //  conf.setClass("fs.file.impl", classOf[LocalFileSystem], classOf[FileSystem])



    def get_files(source_path:Path) : DataFrame = {

         conf.set("hadoop.native.lib", "false") 
         conf.setClass("fs.file.impl", classOf[LocalFileSystem], classOf[FileSystem])

         // List files and directories using LocalFileSystem
         val fs = FileSystem.getLocal(conf)

         val statuses = fs.listStatus(source_path)
         
         // Convert to a DataFrame
         val pathsDF = sc.parallelize(statuses.map(x => (x.getPath().toString(),x.getModificationTime().toString()))).toDF("path","modification_time")

         pathsDF
      }


      def runCopy(src_df:DataFrame) ={
            src_df.rdd.repartition(4).foreachPartition {
            rows => {
                  val partitionId = TaskContext.getPartitionId
                  println("Partition Id: " + partitionId)
                  rows.foreach {
                     file => {

                     val src_path:String = file.get(0).asInstanceOf[String]

                     val fromPath = new Path(src_path)
                     val fromFs = FileSystem.getLocal(conf)
                     val toFs = FileSystem.get(conf)

                     FileUtil.copy(fromFs, fromPath, toFs, hdfs_destination_path, true, conf)
                     }

                  } 
               }
            }

         }
      // runCopy(get_files(source_path = src_file_path))
     
      val fs = FileSystem.get(conf)

      val status = fs.listStatus(hdfs_destination_path)

     // Iterate through the files and print their paths
      status.foreach { status =>
         if (status.isDirectory) {
               println(s"Processing File: ${status.getPath}")

               val  df = (spark.read
                              .format("json")
                              .option("multiline",true)
                              .option("inferschema",true)
                              .option("mode","PERMISSIVE")
                              .load(s"${status.getPath}")
                              )
               println(f"Writing to Delta for" + s"${status.getPath}".split("/").last)
               df.write.format("delta").mode("overwrite").save("hdfs://localhost:9000/process/" + s"${status.getPath}".split("/").last)
         }
      }
      
}    