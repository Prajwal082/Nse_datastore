import org.apache.hadoop.hdfs
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType}
import io.delta.tables
import org.apache.hadoop.fs.{FileUtil,Path,FileSystem,LocalFileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j._


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

    val hdfs_destination_path = new Path( "hdfs://localhost:9000/raw/GLAND/")

    // Hadoop configuration to use LocalFileSystem explicitly
    val conf = new Configuration()

    // Set to your HDFS namenode address
    conf.set("fs.defaultFS", "hdfs://localhost:9000") 

    def get_files(source_path:Path) : DataFrame = {

         conf.setBoolean("hadoop.native.lib", false) 
         conf.setClass("fs.file.impl", classOf[LocalFileSystem], classOf[FileSystem])

         // List files and directories using LocalFileSystem
         val fs = FileSystem.getLocal(conf)

         val statuses = fs.listStatus(source_path)
         
         // Convert to a DataFrame
         val pathsDF = sc.parallelize(statuses.map(x => (x.getPath().toString(),x.getModificationTime().toString()))).toDF("path","modification_time")

         pathsDF
      }

      val df = get_files(source_path = src_file_path)

      df.show()

      // def copy_Files(source_path : Path,destination_path : Path) = {
      // }

      df.repartition(2).foreachPartition(row => println(row.get(0),row.get(1)))
    }