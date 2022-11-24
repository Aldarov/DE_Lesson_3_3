import org.apache.spark.sql.SparkSession

object App {
  def main(array: Array[String]): Unit = {
    println("Hello")
    val spark = SparkSession.builder().master("local[1]")
      .appName("Demo")
      .getOrCreate()

    val data = spark.sparkContext.parallelize(Seq(1,2,3,4,5))
    data.foreach(println)
  }
}
