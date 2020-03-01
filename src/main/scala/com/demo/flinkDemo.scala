package com.demo

import java.util

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

object flinkDemo {


  def main(args: Array[String]): Unit = {
     val env = ExecutionEnvironment.getExecutionEnvironment
     val dataSet: DataSet[String] = env.readTextFile("/Users/bianpeng/Code/testData")
     import org.apache.flink.api.scala.createTypeInformation
     val restlt: AggregateDataSet[(String, Int)] = dataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
     restlt.print()
  }
}
