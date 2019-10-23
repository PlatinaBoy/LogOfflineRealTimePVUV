package main.scala.offlinedownandpvuv


import main.scala.ConfigHelp.ConfigHelper
import main.scala.beans.Logs
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

/**
  * ClassName: logdownbyscalalikejdbc <br/>
  * Description:使用scallikejdbc将数据写入mysql，需要注意的是需要先建表 <br/>
  * date: 2019/5/8 16:47<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object logdownbyscalalikejdbc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    val logpath: RDD[String] = sc.textFile(ConfigHelper.logPath)
    val filted: RDD[Array[String]] = logpath.map(_.split("\\^A", -1))
    val value: RDD[Logs] = filted.map(arr => new Logs(
      arr(0).substring(arr(0).indexOf("="+2)).trim,
      arr(1).substring(arr(1).indexOf("="+1)).trim,
      arr(2).substring(arr(2).indexOf("="+1)).trim,
      arr(3).substring(arr(3).indexOf("="+1)).trim,
      arr(4).substring(arr(4).indexOf("="+1)).trim,
      arr(5).substring(arr(5).indexOf("="+1)).trim,
      arr(6).substring(arr(6).indexOf("="+1)).trim,
      arr(7).substring(arr(7).indexOf("="+1)).trim,
      arr(8).substring(arr(8).indexOf("="+1)).trim,
      arr(9).substring(arr(9).indexOf("="+1)).trim,
      arr(10).substring(arr(10).indexOf("=" +1)).trim,
      arr(11).substring(arr(11).indexOf("=" +1)).trim,
      arr(12).substring(arr(12).indexOf("=" +1)).trim,
      arr(13).substring(arr(13).indexOf("=" +1)).trim,
      arr(14).substring(arr(14).indexOf("=" +1)).trim,
      arr(15).substring(arr(15).indexOf("=" +1)).trim,
      arr(16).substring(arr(16).indexOf("=" +1)).trim,
      arr(17).substring(arr(17).indexOf("=" +1)).trim,
      arr(18).substring(arr(18).indexOf("=" +1)).trim,
      arr(19).substring(arr(19).indexOf("=" +1)).trim,
      arr(20).substring(arr(20).indexOf("=" +1)).trim,
      arr(21).substring(arr(21).indexOf("=" +1)).trim,
      arr(22).substring(arr(22).indexOf("=" +1)).trim,
      arr(23).substring(arr(23).indexOf("=" +1)).trim,
      arr(24).substring(arr(24).indexOf("=" +1)).trim
    ))
    val frame = sQLContext.createDataFrame(value)
    import sQLContext.implicits._
    DBs.setup()
    frame.foreachPartition(partition => {
      DB.localTx { implicit session =>
        partition.foreach(row => {
          SQL("insert into log values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(
              row.getAs[String]("time"),
              row.getAs[String]("event"),
              row.getAs[String]("remoteaddr"),
              row.getAs[String]("url"),
              row.getAs[String]("title"),
              row.getAs[String]("ref"),
              row.getAs[String]("uv_flag"),
              row.getAs[String]("appid"),
              row.getAs[String]("channel"),
              row.getAs[String]("ver"),
              row.getAs[String]("uuid"),
              row.getAs[String]("cid"),
              row.getAs[String]("sub_id"),
              row.getAs[String]("sub_name"),
              row.getAs[String]("u_amount"),
              row.getAs[String]("u_payment"),
              row.getAs[String]("u_currency"),
              row.getAs[String]("donator"),
              row.getAs[String]("u_telephone"),
              row.getAs[String]("email"),
              row.getAs[String]("u_action"),
              row.getAs[String]("kv_value"),
              row.getAs[String]("kv_key"),
              row.getAs[String]("kv_val"),
              row.getAs[String]("kv_info")
            ).update().apply()
        })
      }
    })
    sc.stop()
  }

}
