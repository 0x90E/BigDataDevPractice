package partition.practice


import java.net.URI

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration


class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any = {
    NullWritable.get()
  }

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    (key + "/" + name)
  }
}


object PartitionPracticeFourA {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("PartitionPracticeFourA").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val conf = new Configuration()
    val fileSystem = FileSystem.get(new URI("file:///Users/chung/testFile/"), conf)
    val outputDir = "file:///Users/chung/testFile/output"

    try {
      if (fileSystem.exists(new Path(outputDir))) {
        fileSystem.delete(new Path(outputDir), true)
      }

      sc.textFile("file:///Users/chung/testFile/emp.txt").map(_.split("\t")).filter(_.length == 8).map(line => {
        val deptNo = line(7).replaceAll(" ", "")
        ("deptNo=" + deptNo, line.mkString("\t"))
      }).partitionBy(new HashPartitioner(3)).saveAsHadoopFile("file:///Users/chung/testFile/output",
        classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

    } finally {
      fileSystem.close()
      sc.stop()
    }
  }
}
