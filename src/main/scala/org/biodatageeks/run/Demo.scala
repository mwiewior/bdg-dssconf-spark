package org.biodatageeks.run

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.biodatageeks.catalyst.IntervalTreeJoinStrategy

import scala.util.Random



object Demo {
  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    println("time: " + (System.nanoTime - s) / 1e9 + " seconds")
    ret
  }

  //Define test data
  val tartifSchema = StructType(
    Seq(
      StructField("tarifId",StringType ),
      StructField("start",IntegerType ),
      StructField("end", IntegerType)
    )
  )

  val eventSchema = StructType(
    Seq(
      StructField("eventType",StringType ),
      StructField("start",IntegerType ),
      StructField("end", IntegerType)
    )
  )


  // create a sparkession and inject our strategy
  val spark = SparkSession
    .builder()
    .withExtensions(e => {e.injectPlannerStrategy(IntervalTreeJoinStrategy)})
    .master("local[4]")
    .getOrCreate()


  val tarifRdd1 =  spark.sparkContext.parallelize(
    (1 to 10000)
      .map(
        r=> {
          val rnd = Random.nextInt(86400)
          Row(rnd.toString,rnd,rnd+100)
        }
      )
  )
  val ds1 = spark
    .sqlContext
    .createDataFrame(tarifRdd1,tartifSchema)
  ds1.createOrReplaceTempView("tarif")

  val eventsNum = 500000
  val events = spark
    .sparkContext
    .parallelize(
      (1 to eventsNum)
        .map(
          r=> {
            val rnd = Random.nextInt(86400)
            Row(if(rnd % 2 == 0) "call" else "sms",rnd,rnd)
          }
        )
    )
  val eventDf = spark.sqlContext.createDataFrame(events,eventSchema)
  eventDf.createOrReplaceTempView("cdr")

  def main(args: Array[String]): Unit = {
    val query =
      """
        |SELECT tarifId,COUNT(*) AS CNT
        |FROM
        |cdr c JOIN tarif t
        |ON
        |(c.end>=t.start and c.start<=t.end )
        |GROUP BY tarifId order by tarifId
      """.stripMargin


    spark
      .sql(query)
      .explain

    time(spark
      .sql(query)
      .show(5)
    )
  }

}
