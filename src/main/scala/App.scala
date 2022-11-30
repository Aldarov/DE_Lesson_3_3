import org.apache.spark.sql.functions.{col, from_unixtime, udf, unix_timestamp, when}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, LongType, StringType, StructType}

import java.sql.Date

object App {

  def main(array: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]")
      .appName("Demo")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = new StructType()
      .add("user_id", IntegerType)
      .add("timestamp", LongType)
      .add("type", StringType)
      .add("page_id", IntegerType)
      .add("tag", StringType)
      .add("sign", BooleanType)

    val data = Seq(
      Row(1, 1667257200l, "click", 101, "sport", false),
      Row(1, 1667347200l, "scroll", 102, "business", false),
      Row(2, 1667347200l, "move", 102, "business", true),
      Row(3, 1667440800l, "click", 101, "sport", true),
      Row(3, 1667444400l, "scroll", 102, "business", true),
      Row(3, 1667448000l, "visit", 103, "politics", true),
      Row(3, 1667444400l, "click", 104, "medic", true),
      Row(4, 1667440800l, "move", 103, "politics", false),
      Row(5, 1667462400l, "scroll", 104, "medic", true),
      Row(6, 1667462400l, "visit", 105, "sport", true),
      Row(7, 1667469600l, "click", 102, "business", true),
      Row(8, 1667469600l, "visit", 103, "politics", true),
      Row(9, 1667476800l, "scroll", 102, "business", true),
      Row(10, 1667476800l, "click", 103, "politics", false),
      Row(11, 1667484000l, "click", 103, "politics", true),
      Row(12, 1667484000l, "move", 105, "sport", true),
      Row(12, 1667491200l, "click", 106, "sport", true),
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    println()
    println("Топ-5 самых активных посетителей сайта:")
    df.groupBy("user_id").count()
      .sort(col("count").desc)
      .show(5)

    print("Процент посетителей, у которых есть ЛК: ")
    val countUsers = df.groupBy("user_id").count().count()
    val countSignUsers = df.filter(df("sign") === true)
      .groupBy("user_id").count()
      .count()
    val procSignUsers = countSignUsers.toDouble * 100 / countUsers
    println(procSignUsers.round)

    println()
    println("Топ-5 страниц сайта по показателю общего кол-ва кликов на данной странице:")
    df.withColumn("clicked",
        when(col("type") === "click", 1)
          .otherwise(0)
      )
      .groupBy("page_id").sum("clicked")
      .sort(col("sum(clicked)").desc)
      .show(5)

    println("Топ временных промежутков, в течение которого было больше всего активностей на сайте")
    df.select(
      col("user_id"),
      from_unixtime(col("timestamp"), "HH").as("hour").cast(IntegerType)
    )
    .withColumn("period",
      when(col("hour") >= 0 && col("hour") < 4 , "0-4")
      .when(col("hour") >= 4 && col("hour") < 8 , "4-8")
      .when(col("hour") >= 8 && col("hour") < 12 , "8-12")
      .when(col("hour") >= 12 && col("hour") < 16 , "12-16")
      .when(col("hour") >= 16 && col("hour") < 20 , "16-20")
      .when(col("hour") >= 20 && col("hour") < 24 , "20-24")
    )
    .groupBy("period").count()
    .sort(col("count").desc)
    .show()

    val schemaPersonalArea = new StructType()
      .add("id", IntegerType)
      .add("user_id", IntegerType)
      .add("fio", StringType)
      .add("birth_date", DateType)
      .add("create_date", DateType)

    val users = Seq(
      Row(1, 1, "Иванов И.С.", Date.valueOf("1990-03-01"), Date.valueOf("2019-09-01")),
      Row(2, 2, "Петров А.С.", Date.valueOf("1990-03-23"), Date.valueOf("2019-09-01")),
      Row(3, 3, "Сидоров П.С.", Date.valueOf("1991-03-01"), Date.valueOf("2019-09-01")),
      Row(4, 4, "Макарова С.С.", Date.valueOf("1993-06-01"), Date.valueOf("2019-09-01")),
      Row(5, 5, "Сергеев И.С.", Date.valueOf("1994-07-01"), Date.valueOf("2019-09-01")),
      Row(6, 6, "Матвеев А.С.", Date.valueOf("1990-04-11"), Date.valueOf("2019-09-01")),
      Row(7, 7, "Николаев Р.Н.", Date.valueOf("1997-03-11"), Date.valueOf("2019-09-01")),
      Row(8, 8, "Миронова С.А.", Date.valueOf("1999-03-21"), Date.valueOf("2019-09-01")),
      Row(9, 9, "Чупкин М.М.", Date.valueOf("1997-03-11"), Date.valueOf("2019-09-01")),
      Row(10, 10, "Васильев М.И.", Date.valueOf("1996-03-01"), Date.valueOf("2019-09-01")),
      Row(11, 11, "Дагбаева П.Н.", Date.valueOf("1995-03-01"), Date.valueOf("2019-09-01")),
      Row(12, 12, "Суворов А.С.", Date.valueOf("1994-03-01"), Date.valueOf("2019-09-01")),
    )

    val usersDF = spark.createDataFrame(spark.sparkContext.parallelize(users), schemaPersonalArea)

    println("Фамилии посетителей, которые читали хотя бы одну новость про спорт.")
    df.join(usersDF, df("user_id") === usersDF("user_id"), "inner")
      .filter(df("tag") === "sport")
      .select(col("fio"))
      .distinct()
      .show()

    println("10% ЛК, у которых максимальная разница между датой создания ЛК и датой последнего посещения:")
    df.join(usersDF, df("user_id") === usersDF("user_id"), "inner")
      .select(col("id"), col("fio"), (col("timestamp") - unix_timestamp(col("create_date"))).alias("diff_visit_date"))
      .groupBy("id", "fio").max("diff_visit_date")
      .sort(col("max(diff_visit_date)").desc)
      .show(2)

    println("Топ-5 страниц, которые чаще всего посещают мужчины и топ-5 страниц, которые посещают чаще женщины.")

    def calc_gender = (fio: String) => {
      val fullName = fio.split (" ")
      if (fullName (0).takeRight (2) == "ов" || fullName (0).takeRight (2) == "ев" || fullName (0).takeRight (2) == "ин") {
        "М"
      } else {
        "Ж"
      }
    }
    val calc_gender_udf = udf(calc_gender)
    usersDF.join(usersDF, df("user_id") === usersDF("user_id"), "inner")
      .withColumn("gender", calc_gender_udf(col("fio")))
      .groupBy("page_id", "gender").count()
      .withColumn("max_cnt", max(col("count")))
      .show()
  }
}
