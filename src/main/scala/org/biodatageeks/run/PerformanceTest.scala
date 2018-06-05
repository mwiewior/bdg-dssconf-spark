package org.biodatageeks.run

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.biodatageeks.catalyst.IntervalTreeJoinStrategy

import scala.util.Random

object PerformanceTest {

    def time[A](f: => A) = {
        val s = System.nanoTime
        val ret = f
        println("time: " + (System.nanoTime - s) / 1e9 + " seconds")
        ret
    }


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

    def main(args: Array[String]): Unit = {

        val sparkSession = SparkSession
          .builder()
            .master("local[4]")
          .getOrCreate()

        sparkSession.sparkContext.setLogLevel("ERROR")

        sparkSession.conf.set("spark.driver.memory", "4g")

        val tarifRdd1 =  sparkSession.sparkContext.parallelize(
        (1 to 10000)
          .map(
            r=> {
              val rnd = Random.nextInt(86400)
              Row(rnd.toString,rnd,rnd+100)
            }
          )
      )
        val ds1 = sparkSession
          .sqlContext
          .createDataFrame(tarifRdd1,tartifSchema)
        ds1.createOrReplaceTempView("tarif")

        val eventsNum = 500000
        val events = sparkSession
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
        val eventDf = sparkSession.sqlContext.createDataFrame(events,eventSchema)
        eventDf.createOrReplaceTempView("cdr")
        //eventDf.cache().count

        ds1.show(3)
        eventDf.show(3)
        sparkSession.experimental.extraStrategies = Nil

        val query =
            """
              |SELECT tarifId,COUNT(*) AS CNT
              |FROM
              |cdr c JOIN tarif t
              |ON
              |(c.end>=t.start and c.start<=t.end )
              |GROUP BY tarifId order by tarifId
            """.stripMargin

            println("Running a default strategy...")
            time(sparkSession
              .sql(query)
               // .explain
              .show(5)
            )

            sparkSession.experimental.extraStrategies = new IntervalTreeJoinStrategy(sparkSession) :: Nil
            println("Running an optimized strategy...")
            time(sparkSession
              .sql(query)
               // .explain()
              .show(5)
            )
    }



}

