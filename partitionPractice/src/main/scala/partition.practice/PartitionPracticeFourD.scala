package partition.practice

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable


object PartitionPracticeFourD {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("PartitionPracticeFourD").setMaster("local[2]").
      set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(sparkConf)
    val conf = new Configuration()
    val fileSystem = FileSystem.get(new URI("file:///Users/chung/testFile/"), conf)
    val outputDir = "file:///Users/chung/testFile/outputD/"

    try {
      // emp.txt
      // emp_hireDay.txt
      val rdd = sc.textFile("file:///Users/chung/testFile/emp.txt").map(_.split("\t")).
        filter(_.length == 8).map(line => {
          val deptNo = line(7).replaceAll(" ", "")
          val key = "deptNo=" + deptNo + "/" + line(4).split("-")(0)
          (key, line.mkString("\t"))
      })

      val rddMap = rdd.collectAsMap()
      val partitionsSet = new mutable.HashSet[String]()
      rddMap.map(_._1).foreach((x: String) => partitionsSet.add(x))

      partitionsSet.foreach(partition => {
        println("path: " + outputDir + partition)
        SparkHadoopUtil.get.globPath(new Path(outputDir + partition)).map(fileSystem.delete(_, true))
      })

      rdd.saveAsHadoopFile(outputDir, classOf[String],
        classOf[String], classOf[RDDMultipleTextOutputFormat])
    } finally {
      fileSystem.close()
      sc.stop()
    }
  }
}
