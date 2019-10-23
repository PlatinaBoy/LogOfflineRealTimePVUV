package main.scala.beans

/**
  * ClassName: Logs <br/>
  * Description:设置字段数大于23 <br/>
  * date: 2019/4/28 11:25<br/>
  *
  * @author PlatinaBoy<br/>
  * @version
  */


class Logs(
            val time: String,
            val event: String,
            val remoteaddr: String,
            val url: String,
            val title: String,
            val ref: String,
            val uv_flag: String,
            val appid: String,
            val channel: String,
            val ver: String,
            val uuid: String,
            val cid: String,
            val sub_id: String,
            val sub_name: String,
            val u_amount: String,
            val u_payment: String,
            val u_currency: String,
            val donator: String,
            val u_telephone: String,
            val email: String,
            val u_action: String,
            val kv_value: String,
            val kv_key: String,
            val kv_val: String,
            val kv_info: String


          ) extends Product {
  override def productElement(n: Int): Any = n match {
    case 0 => time
    case 1 => event
    case 2 => remoteaddr
    case 3 => url
    case 4 => title
    case 5 => ref
    case 6 => uv_flag
    case 7 => appid
    case 8 => channel
    case 9 => ver
    case 10 => uuid
    case 11 => cid
    case 12 => sub_id
    case 13 => sub_name
    case 14 => u_amount
    case 15 => u_payment
    case 16 => u_currency
    case 17 => donator
    case 18 => u_telephone
    case 19 => email
    case 20 => u_action
    case 21 => kv_value
    case 22 => kv_key
    case 23 => kv_val
    case 24 => kv_info

    case _ => ""
  }

  override def productArity: Int = 25

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Logs]


}
