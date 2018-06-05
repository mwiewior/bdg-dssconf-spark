package org.biodatageeks.catalyst

import java.util.logging.Logger

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.JavaConversions._

object IntervalTreeJoinOptimImpl extends Serializable {

  val logger =  Logger.getLogger(this.getClass.getCanonicalName)

  /**
    *
    * @param sc
    * @param rdd1
    * @param rdd2
    * @param rdd1Count
    * @return
    */
  def overlapJoin(sc: SparkContext,
                  rdd1: RDD[(IntervalWithRow[Int])],
                  rdd2: RDD[(IntervalWithRow[Int])], rdd1Count:Long ): RDD[(InternalRow, InternalRow)] = {

    /* Collect only Reference regions and the index of indexedRdd1 */


    /**
      * Broadcast join pattern - use if smaller RDD is narrow otherwise follow 2-step join operation
      * first zipWithIndex and then perform a regular join
      */

      val localIntervals =
        rdd1
          .map(r=>IntervalWithRow(r.start,r.end,r.row.copy()) )
          .collect()
      val intervalTree = {
        val tree = new IntervalTreeHTS[InternalRow]()
        localIntervals
          .foreach(r => tree.put(r.start, r.end, r.row))
        sc.broadcast(tree)
      }


      val kvrdd2 = rdd2
        .mapPartitions(p => {
          p.map(r => {
              val record =
                intervalTree.value.overlappers(r.start, r.end)
              record
                .flatMap(k => (k.getValue.map(s=>(s,r.row))) )
          })
        })
        .flatMap(r => r)
      kvrdd2
    }


}