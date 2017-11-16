package partition.practice

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object PartitionPracticeFourBC {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("PartitionPracticeFourBC").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val conf = new Configuration()
    val fileSystem = FileSystem.get(new URI("file:///Users/chung/testFile/"), conf)
    val outputDir = "file:///Users/chung/testFile/outputBC"

    try {
      if (fileSystem.exists(new Path(outputDir))) {
        fileSystem.delete(new Path(outputDir), true)
      }

      val rdd = sc.textFile("file:///Users/chung/testFile/emp_skew.txt").map(_.split("\t")).
        filter(_.length == 8).map(line => {
          val deptNo = line(7).replaceAll(" ", "")
          (deptNo, line.mkString("\t"))
      })

      val skewedNo = rdd.sample(false, 1).countByKey().map(x=> (x._2, x._1)).max._2
      println("skewedNo: " + skewedNo)
      val skewedPairRDD = rdd.filter(x => skewedNo.equals(x._1))
      val normalPairRDD = rdd.filter(x => !skewedNo.equals(x._1))

      val finalPairRDD = skewedPairRDD.coalesce(2).union(normalPairRDD.partitionBy(new HashPartitioner(2)))
      finalPairRDD.saveAsHadoopFile(outputDir, classOf[String], classOf[String],
        classOf[RDDMultipleTextOutputFormat])
    } finally {
      fileSystem.close()
      sc.stop()
    }
  }
}
