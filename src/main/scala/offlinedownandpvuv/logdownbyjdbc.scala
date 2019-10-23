package main.scala.logdown

import java.util.Properties

import main.scala.ConfigHelp.ConfigHelper
import main.scala.beans.Logs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ClassName: logdown <br/>
  * Description:使用javajdbc将数据写入mysql，这样的方法不需要提前建表。<br/>
  * date: 2019/5/8 10:30<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object logdownbyjdbc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    import sQLContext.implicits._
    val frame: RDD[Logs] = sc.textFile(ConfigHelper.logPath).map(line => {

      val log: Array[String] = line.split("\\^A", -1)
      val logs: Logs = new Logs(
        //a.substring(a.indexOf(",")+1).trim()
        log(0).substring(log(0).indexOf("=") + 1).trim,
        log(1).substring(log(1).indexOf("=") + 1).trim,
        log(2).substring(log(2).indexOf("=") + 1).trim,
        log(3).substring(log(3).indexOf("=") + 1).trim,
        log(4).substring(log(4).indexOf("=") + 1).trim,
        log(5).substring(log(5).indexOf("=") + 1).trim,
        log(6).substring(log(6).indexOf("=") + 1).trim,
        log(7).substring(log(7).indexOf("=") + 1).trim,
        log(8).substring(log(8).indexOf("=") + 1).trim,
        log(9).substring(log(9).indexOf("=") + 1).trim,
        log(10).substring(log(10).indexOf("=") + 1).trim,
        log(11).substring(log(11).indexOf("=") + 1).trim,
        log(12).substring(log(12).indexOf("=") + 1).trim,
        log(13).substring(log(13).indexOf("=") + 1).trim,
        log(14).substring(log(14).indexOf("=") + 1).trim,
        log(15).substring(log(15).indexOf("=") + 1).trim,
        log(16).substring(log(16).indexOf("=") + 1).trim,
        log(17).substring(log(17).indexOf("=") + 1).trim,
        log(18).substring(log(18).indexOf("=") + 1).trim,
        log(19).substring(log(19).indexOf("=") + 1).trim,
        log(20).substring(log(20).indexOf("=") + 1).trim,
        log(21).substring(log(21).indexOf("=") + 1).trim,
        log(22).substring(log(22).indexOf("=") + 1).trim,
        log(23).substring(log(23).indexOf("=") + 1).trim,
        log(24).substring(log(24).indexOf("=") + 1).trim)
      logs//这里的类 要用case修饰
    })
    val props = new Properties()
    props.setProperty("driver", ConfigHelper.driver)
    props.setProperty("user", ConfigHelper.user)
    props.setProperty("password", ConfigHelper.password)
    //增量写入mysql 采用overwrite，
    frame.toDF().write.mode(SaveMode.Overwrite).jdbc(ConfigHelper.url, "log", props)

  sc.stop()
  }
}
