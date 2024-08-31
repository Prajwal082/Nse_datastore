import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.hadoop.conf.Configuration

object test extends App {
  
    val conf = new Configuration()

    conf.set("fs.defaultFS", "hdfs://localhost:9000")

    val dir = new Path("hdfs://localhost:9000/raw/")
    val fs = FileSystem.get(conf)

    val status = fs.listStatus(dir)

     // Iterate through the files and print their paths
    status.foreach { status =>
      if (status.isFile) {
        println(s"File: ${status.getPath}")
      } else if (status.isDirectory) {
        println(s"Directory: ${status.getPath}")
      }
    }
}
