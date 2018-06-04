package org.biodatageeeks.catalyst

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, _}
import org.apache.spark.sql.internal.SQLConf

@DeveloperApi
case class IntervalTreeJoinOptim(left: SparkPlan,
                                 right: SparkPlan,
                                 condition: Seq[Expression],
                                 context: SparkSession,leftLogicalPlan: LogicalPlan, righLogicalPlan: LogicalPlan) extends BinaryExecNode {
  def output = left.output ++ right.output

  lazy val (buildPlan, streamedPlan) = (left, right)

  lazy val (buildKeys, streamedKeys) = (List(condition(0), condition(1)),
    List(condition(2), condition(3)))

  @transient lazy val buildKeyGenerator = new InterpretedProjection(buildKeys, buildPlan.output)
  @transient lazy val streamKeyGenerator = new InterpretedProjection(streamedKeys,
    streamedPlan.output)

  protected override def doExecute(): RDD[InternalRow] = {
    val v1 = left.execute()
    val v1kv = v1.map(x => {
      val v1Key = buildKeyGenerator(x)

      (new IntervalWithRow[Int](v1Key.getInt(0), v1Key.getInt(1),
        x) )

    })
    val v2 = right.execute()
    val v2kv = v2.map(x => {
      val v2Key = streamKeyGenerator(x)
      (new IntervalWithRow[Int](v2Key.getInt(0), v2Key.getInt(1),
        x) )
    })
    /* As we are going to collect v1 and build an interval tree on its intervals,
    make sure that its size is the smaller one. */

    val conf = new SQLConf()
    val v1Size =
      if(leftLogicalPlan
        .stats(conf)
        .sizeInBytes >0) leftLogicalPlan.stats(conf).sizeInBytes.toLong
      else
        v1.count

    val v2Size = if(righLogicalPlan
      .stats(conf)
      .sizeInBytes >0) righLogicalPlan.stats(conf).sizeInBytes.toLong
    else
      v2.count
    if ( v1Size <= v2Size ) {
      val v3 = IntervalTreeJoinOptimImpl.overlapJoin(context.sparkContext, v1kv, v2kv,v1.count())
      v3.mapPartitions(
        p => {
          val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
          p.map(r=>joiner.join(r._1.asInstanceOf[UnsafeRow],r._2.asInstanceOf[UnsafeRow]))
        }

      )

    }
    else {
      val v3 = IntervalTreeJoinOptimImpl.overlapJoin(context.sparkContext, v2kv, v1kv, v2.count())
      v3.mapPartitions(
        p => {
          val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
          p.map(r=>joiner.join(r._2.asInstanceOf[UnsafeRow],r._1.asInstanceOf[UnsafeRow]))
        }

      )
    }

  }
}
