package com.demo
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object flinkDataStreamDemo {
  def main(args: Array[String]): Unit = {
    val paraTool = ParameterTool.fromArgs(args)
    val host: String = paraTool.get("host")
    val port = paraTool.get("port")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[String] = env.socketTextStream("127.0.0.1",7777)
    import org.apache.flink.api.scala._
    val dataStreamResult: DataStream[(String, Int)] = dataStream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)
    dataStreamResult.print()
    env.execute()
  }
}
