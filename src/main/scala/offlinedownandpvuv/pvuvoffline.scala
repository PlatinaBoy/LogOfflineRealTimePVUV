package main.scala.offlinedownandpvuv

import java.sql.{Connection, DriverManager, PreparedStatement}

import main.scala.ConfigHelp.ConfigHelper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ClassName: pvuvoffline <br/>
  * Description:离线pvuv，可以存储到需要的数据库中，已经通过测试
  * 设计表的时候加入CURRENT_TIMESTAMP字段表示时间属性<br/>
  * date: 2019/5/9 11:15<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object pvuvoffline {

  case class Blog(name: String, count: Int)

  def myFun(iterator: Iterator[(String, Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql =
      """
        |insert into pvuv_table(name, count) values (?,?)
        |
      """.stripMargin

    Class.forName(ConfigHelper.driver).newInstance()
    conn = DriverManager.getConnection(ConfigHelper.url, ConfigHelper.user, ConfigHelper.password)

    iterator.foreach(data => {
      ps = conn.prepareStatement(sql)
      ps.setString(1, data._1)
      ps.setInt(2, data._2)
      ps.executeUpdate()

      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()

      }
    })
  }

  def main(args: Array[String]): Unit = {
    //1、创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")

    //2、创建SparkContext
    val sc = new SparkContext(sparkConf)
    //
    val sQLContext = new SQLContext(sc)

    //3、读取数据文件
    val data: RDD[String] = sc.textFile(ConfigHelper.logPath)

    //4、统计pv
    val pv = data.count().toInt
    //print(pv)

    //5、统计uv
    val ips: RDD[String] = data.map(x => x.split("\\^A")(2))

    //5.1、根据ip地址去重
    val distinctRDD: RDD[String] = ips.distinct()

    //5.2、统计UV
    val uv = distinctRDD.count().toInt
    //print(uv)
    val value: RDD[(String, Int)] = sc.parallelize(List(("pv", pv), ("uv", uv)))

    value.foreachPartition(myFun)
    //5、关闭
    sc.stop()
  }


}
