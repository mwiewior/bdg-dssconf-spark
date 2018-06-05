package org.biodatageeks.tests

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.biodatageeks.catalyst.IntervalTreeJoinStrategy
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.util.Random
class IntervalTreeTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext{

  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    println("time: " + (System.nanoTime - s) / 1e9 + " seconds")
    ret
  }


  val tartifSchema = StructType(
    Seq(
      StructField("tarifName",StringType ),
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



  val query =
    """
      |SELECT tarifName,eventType,COUNT(*) AS CNT
      |FROM
      |cdr c JOIN tarif t
      |ON
      |(c.end>=t.start and c.start<=t.end )
      |GROUP BY tarifName,eventType
    """.stripMargin


  before{
    val tarifRdd1 = sc.parallelize(Seq(
      ("day",0,43200),
      ("night",43201, 86400))
      .map(i => Row(i._1, i._2, i._3)))

    val ds1 = spark
      .sqlContext
      .createDataFrame(tarifRdd1,tartifSchema)

    ds1.createOrReplaceTempView("tarif")

    val cdrRdd = sc.parallelize(Seq(
      ("call",150, 152),
      ("sms",22000, 22000),
      ("call",50000, 50015)))
      .map(i => Row(i._1, i._2, i._3))

    val ds2 =
      sqlContext
      .createDataFrame(cdrRdd,eventSchema)

    ds2.createOrReplaceTempView("cdr")



  }
  test ("Test analytical query - a default approach"){

    sqlContext
      .sql(query)
      .explain()
  }


  test ("Test analytical query - an optimized approach"){

    spark.experimental.extraStrategies = new IntervalTreeJoinStrategy(spark) :: Nil
    sqlContext
      .sql(query)
      .explain()
  }

  test("Give me some numbers"){

    val eventsNum = 5000000
    val exents = sc.parallelize(
      (1 to eventsNum)
        .map(
          r=> {
            val rnd = Random.nextInt(86400)
            Row(if(rnd % 2 == 0) "call" else "sms",rnd,rnd)
          }
        )
    )
    val eventDf = sqlContext.createDataFrame(exents,eventSchema)
    eventDf.createOrReplaceTempView("cdr")
    //eventDf.cache().count

    spark.experimental.extraStrategies = Nil

    val query =
      """
        |SELECT tarifName,COUNT(*) AS CNT
        |FROM
        |cdr c JOIN tarif t
        |ON
        |(c.end>=t.start and c.start<=t.end )
        |GROUP BY tarifName
      """.stripMargin

//    time(spark
//      .sql(query)
//      .show)
//
//    spark.experimental.extraStrategies = new IntervalTreeJoinStrategy(spark) :: Nil
//
//    time(spark
//      .sql(query)
//      .show)


  }

}
