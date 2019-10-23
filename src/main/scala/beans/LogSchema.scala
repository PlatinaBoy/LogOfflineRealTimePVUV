package main.scala.beans

import org.apache.spark.sql.types._

/**
  * ClassName: LogSchema <br/>
  * Description:LogSchema <br/>
  * date: 2019/4/28 11:29<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */
object LogSchema {
  val schema = StructType(Seq(
    StructField("time", StringType),
    StructField("event", StringType),
    StructField("remoteaddr", StringType),
    StructField("url", StringType),
    StructField("title", StringType),
    StructField("ref", StringType),
    StructField("uv_flag", StringType),
    StructField("appid", StringType),
    StructField("channel", StringType),
    StructField("ver", StringType),
    StructField("uuid", StringType),
    StructField("cid", StringType),
    StructField("sub_id", StringType),
    StructField("sub_name", StringType),
    StructField("u_amount", StringType),
    StructField("u_payment", StringType),
    StructField("u_currency", StringType),
    StructField("donator", StringType),
    StructField("u_telephone", StringType),
    StructField("email", StringType),
    StructField("u_action", StringType),
    StructField("kv_value", StringType),
    StructField("kv_key", StringType),
    StructField("kv_val", StringType),
    StructField("kv_info", StringType)


  ))
}