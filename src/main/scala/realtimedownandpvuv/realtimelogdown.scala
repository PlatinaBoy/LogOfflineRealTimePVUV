package main.scala.realtimedownandpvuv

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONException}
import main.scala.ConfigHelp.ConfigHelper
import main.scala.beans.Logbean
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}


/**
  * ClassName: realtimelogdown <br/>
  * Description: spark+kafka+mysql实现数据存入到mysql<br/>
  * date: 2019/5/9 13:58<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object realtimelogdown {

  def main(args: Array[String]): Unit = {
    // 减少日志输出
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //    val spark = SparkSession.builder()
    //      .appName("RealtimeEtl")
    //      .master("local[*]")
    //      .getOrCreate()

    //val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(10))

    //直连方式相当于跟Kafka的Topic直接连接
    //使用Kafka底层的消费API
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group",
//      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("malog")
    val kafkaDStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //DStream是的RDD的封装，你的DSteam进行操作，本质上的对RDD进行操作
    //val stream1 = kafkaDStream.map(_.value())

    //如果使用SparkStreaming跟Kafka直连方式进行整合,生成的kafkaDStream必须调用foreachRDD
    kafkaDStream.foreachRDD(kafkaRDD => {

      if (!kafkaRDD.isEmpty()) {
        //获取当前批次RDD的偏移量
        val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

        //拿出kafka中的数据
        val lines: RDD[String] = kafkaRDD.map(_.value())
        lines.foreach(print)

//        //将jons字符串转换成JSON对象
//        val logBeanRDD = lines.map(line => {
//
//          var logBean: LogBean = null
//          try {
//            logBean = JSON.parseObject(line, classOf[LogBean])
//          } catch {
//            case e: JSONException => {
//              //logger记录log
//            }
//          }
//          logBean
//        })

        import spark.implicits._
//
//        //过滤
//        val filteredRDD = logBeanRDD.filter(_ != null)
//        //将RDD转换成DataFrame，应为filteredRDD中装的是Case Class
//        val df = filteredRDD.toDF()
//
//        //将数据写入到HDFS中
//        df.repartition(2).write.mode(SaveMode.Append).parquet(args(0))


        //提交当前批次对应的偏移量
        //偏移量的提交是在哪一端执行的呢？，偏移量最后吸入Kafka
        kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

      }

    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}

