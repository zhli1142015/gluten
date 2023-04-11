/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.glutenproject.execution

import com.google.common.collect.Lists
import com.google.protobuf.Any
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.{AttributeReferenceTransformer, ConverterUtils, ExpressionConverter}
import io.glutenproject.extension.GlutenPlan
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.utils.BindReferencesUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util
import scala.util.control.Breaks.{break, breakable}

case class ExpandExecTransformer(projections: Seq[Seq[Expression]],
                                 groupExpression: Seq[NamedExpression],
                                 output: Seq[Attribute],
                                 child: SparkPlan)
  extends UnaryExecNode with TransformSupport with GlutenPlan {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genExpandTransformerMetrics(sparkContext)

  val originalInputAttributes: Seq[Attribute] = child.output

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetricsUpdater(metrics)

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  override def supportsColumnar: Boolean = true

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = child match {
    case c: TransformSupport =>
      c.columnarInputRDDs
    case _ =>
      Seq(child.executeColumnar())
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {
    throw new UnsupportedOperationException(s"This operator doesn't support getBuildPlans.")
  }

  override def getStreamedLeafPlan: SparkPlan = child match {
    case c: TransformSupport =>
      c.getStreamedLeafPlan
    case _ =>
      this
  }

  def getRelNode(context: SubstraitContext,
                 projections: Seq[Seq[Expression]],
                 groupExpression: Seq[NamedExpression],
                 originalInputAttributes: Seq[Attribute],
                 operatorId: Long,
                 input: RelNode,
                 validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // There are three cases we need to handle:
    // 1. agg exprs + group by exprs + gid.
    // 2. agg exprs + group by exprs + gid + _gen_grouping_pos.
    //  --> The last column is for handling duplicate grouping sets.
    // 3. group by exprs + gid + agg exprs.
    //  --> gid is calculated by different way from above two cases.
    val colGroupIdIndex = projections.head
      .indexWhere(p => p.isInstanceOf[Literal] && p.asInstanceOf[Literal].value != null)
    val hasDuplicateGroupingSets = colGroupIdIndex == projections.head.size - 2 &&
      groupExpression.last.name == "_gen_grouping_pos"
    println(s"projections: ${projections}")
    println(s"groupExpression: ${groupExpression}")
    println(s"originalInputAttributes: ${originalInputAttributes}")
    println(s"output: ${output}")
    val extraGroupCols = if (hasDuplicateGroupingSets) 2 else 1
    println(s"extraGroupCols: ${extraGroupCols}")
    val groupSize = groupExpression.size - extraGroupCols
    println(s"groupSize: ${groupSize}")
    val aggSize = projections.head.size - groupExpression.size
    println(s"aggSize: ${aggSize}")

    val groupExprStartIndex = if (colGroupIdIndex != projections.head.size -1 &&
      !hasDuplicateGroupingSets) {
      0
    } else {
      aggSize
    }
    println(s"groupExprStartIndex: ${groupExprStartIndex}")

    val aggExprStartIndex = if (colGroupIdIndex != projections.head.size -1 &&
      !hasDuplicateGroupingSets) {
      colGroupIdIndex + 1
    } else {
      0
    }
    println(s"aggExprStartIndex: ${aggExprStartIndex}")

    val groupsetExprNodes = new util.ArrayList[util.ArrayList[ExpressionNode]]()
    val aggExprNodes = new util.ArrayList[ExpressionNode]()
    for (i <- aggExprStartIndex until aggExprStartIndex + aggSize) {
      var expr: Option[Expression] = None
      for (j <- 0 until projections.size) {
        if (expr.isEmpty && !(projections(j).apply(i).isInstanceOf[Literal] &&
          projections(j).apply(i).asInstanceOf[Literal].value == null)) {
          expr = Some(projections(j).apply(i))
        }
      }
      var aggExprNode = ExpressionConverter
        .replaceWithExpressionTransformer(
          expr.get,
          originalInputAttributes)
        .doTransform(args)
      aggExprNodes.add(aggExprNode)
    }

    projections.foreach { projection =>
      val groupExprNodes = new util.ArrayList[ExpressionNode]()
      // TODO: here we need add flag
      for (i <- groupExprStartIndex until groupExprStartIndex + groupSize + extraGroupCols) {
        if (!(projection(i).isInstanceOf[Literal] &&
          projection(i).asInstanceOf[Literal].value == null)) {
          var groupExprNode = ExpressionConverter
            .replaceWithExpressionTransformer(
              projection(i),
              originalInputAttributes
            )

          groupExprNode match {
            case attrRefTransform: AttributeReferenceTransformer =>
              /*
               * There is a special case, E.g,
               *  select x, y, count(x) from t group by x, y with rollup.
               * The input header for this operator is: x_0, x_1, y, but the reference to x in
               * grouping sets is also 0 (refer to x_0) which may be 1 which would cause some
               * problems. We fix it here.
               */
              if (attrRefTransform.ordinal < aggSize) {
                var index = originalInputAttributes.length - 1
                breakable {
                  while (index >= 0) {
                    if (originalInputAttributes(index).exprId.equals(attrRefTransform.exprId)) {
                      groupExprNode = AttributeReferenceTransformer(attrRefTransform.name,
                        index, attrRefTransform.dataType, attrRefTransform.nullable,
                        attrRefTransform.exprId, attrRefTransform.qualifier,
                        attrRefTransform.metadata)
                      break
                    }
                    index -= 1
                  }
                }
              }
            case _ =>
          }
          val transformedNode = groupExprNode.doTransform(args)
          groupExprNodes.add(transformedNode)
        }
      }
      groupsetExprNodes.add(groupExprNodes)
    }

    val expandRel = if (!validation) {
      RelBuilder.makeExpandRel(
        input,
        "spark_grouping_id", groupsetExprNodes, aggExprNodes,
        context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for a validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(
          ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }

      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeExpandRel(input,
        "spark_grouping_id", groupsetExprNodes, aggExprNodes,
        extensionNode, context, operatorId)
    }
    println("validated for expand")

    if (BackendsApiManager.getSettings.needProjectExpandOutput) {
      // After ExpandRel in velox, the output is
      // grouping cols + agg cols + groupingID col,
      // here we need to add ProjectRel to
      // convert the ouput to the correct order.
      val selectNodes = new java.util.ArrayList[ExpressionNode]()
      def AddGroupIdCol(
        selectNodes: java.util.ArrayList[ExpressionNode],
        gidIndex: Int,
        isIntegerType: Boolean = false): Unit = {
        if (SQLConf.get.integerGroupingIdEnabled || isIntegerType) {
          // When 'integerGroupingIdEnabled' set, groupID is integer type but velox always
          // gerenate long type value. Convert result to fix test case 'SPARK-30279 Support 32 or
          // more grouping attributes for GROUPING_ID()'.
          val typeNode = ConverterUtils.getTypeNode(DataTypes.IntegerType, false)
          selectNodes.add(
            ExpressionBuilder.makeCast(
              typeNode,
              ExpressionBuilder.makeSelection(gidIndex),
              SQLConf.get.ansiEnabled))
        } else {
          selectNodes.add(ExpressionBuilder.makeSelection(gidIndex))
        }
      }

      if (groupExprStartIndex < aggExprStartIndex) {
        // Add grouping cols index
        for (i <- 0 until groupSize) {
          println(i)
          selectNodes.add(ExpressionBuilder.makeSelection(i))
        }
        // Add groupID col index
        println(projections(0).size - 1)
        AddGroupIdCol(selectNodes, projections(0).size - 1, isIntegerType = true)
        // Add agg cols index
        for (i <- 0 until aggSize) {
          println(i + groupSize)
          selectNodes.add(ExpressionBuilder.makeSelection(i + groupSize))
        }
      } else {
        // Add agg cols index
        for (i <- 0 until aggSize) {
          selectNodes.add(ExpressionBuilder.makeSelection(i + groupSize))
        }
        // Add grouping cols index
        for (i <- 0 until groupSize) {
          selectNodes.add(ExpressionBuilder.makeSelection(i))
        }
        // Add groupID col index
        AddGroupIdCol(selectNodes, projections(0).size - 1)
        if (hasDuplicateGroupingSets) {
          selectNodes.add(
            ExpressionBuilder.makeSelection(projections(0).size - extraGroupCols))
        }
      }
      // Pass the reordered index agg + groupingsets + GID
      val emitStartIndex = projections.head.size
      if (!validation) {
        RelBuilder.makeProjectRel(expandRel, selectNodes, context, operatorId, emitStartIndex)
      } else {
        // Use a extension node to send the input types through Substrait plan for a validation.
        val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
        for (attr <- output) {
          inputTypeNodeList.add(
            ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        }

        val extensionNode = ExtensionBuilder.makeAdvancedExtension(
          Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
        RelBuilder.makeProjectRel(
          expandRel, selectNodes, extensionNode, context, operatorId, emitStartIndex)
      }
    } else {
      expandRel
    }
  }

  override def doValidateInternal(): Boolean = {
    if (!BackendsApiManager.getSettings.supportExpandExec()) {
      return false
    }
    if (projections.isEmpty) {
      return false
    }

    // FIXME.
    // There is a bad case in `Gluten null count` in GlutenDataFrameAggregateSuite, but there may
    // be also other cases which we don't meet.
    if (child.output.size + 1 != projections.head.size) {
      logWarning(s"Not a supported expand node for grouping sets.")
      //return false
    }

    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId

    val relNode = try {
      getRelNode(
        substraitContext,
        projections, groupExpression,
        child.output, operatorId, null, validation = true)
    } catch {
      case e: Throwable =>
        logValidateFailure(
          s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}", e)
        return false
    }

    if (relNode != null && GlutenConfig.getConf.enableNativeValidation) {
      val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
      BackendsApiManager.getValidatorApiInstance.doValidate(planNode)
    } else {
      true
    }
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child match {
      case c: TransformSupport =>
        c.doTransform(context)
      case _ =>
        null
    }

    val operatorId = context.nextOperatorId
    if (projections == null || projections.isEmpty) {
      // The computing for this Expand is not needed.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val (currRel, inputAttributes) = if (childCtx != null) {
      (getRelNode(
        context,
        projections, groupExpression,
        child.output, operatorId, childCtx.root, validation = false),
        childCtx.outputAttributes)
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      val attrList = new util.ArrayList[Attribute]()
      for (attr <- child.output) {
        attrList.add(attr)
      }
      val readRel = RelBuilder.makeReadRel(attrList, context, operatorId)
      (getRelNode(
        context,
        projections, groupExpression,
        child.output, operatorId, readRel, validation = false),
        child.output)
    }
    assert(currRel != null, "Expand Rel should be valid")
    val outputAttrs = BindReferencesUtil.bindReferencesWithNullable(output, inputAttributes)
    TransformContext(inputAttributes, outputAttrs, currRel)
  }

  protected override def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("doExecute is not supported in ColumnarExpandExec.")

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ExpandExecTransformer =
    copy(child = newChild)
}
