import java.sql.DriverManager
import java.util.ArrayList 
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.SubstringComparator
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._
import sqlContext.implicits._
val hc= new HiveContext(sc)
import hc.implicits._


//get metadata from mysql and create dataframe from it  

val sql_meta="select * from v_entity_ap_rel limit ?,?"

val rdd_meta = new org.apache.spark.rdd.JdbcRDD(
 sc,
 () => {
   Class.forName("com.mysql.jdbc.Driver").newInstance()
   DriverManager.getConnection("jdbc:mysql://192.168.2.69:3306/rawdb", "oracle", "oracle")
 },
 sql_meta, 0 ,100000,1,
 r => r.getInt("entityid")+","+r.getString("apmac")+","+r.getInt("rssi")+","+r.getInt("indoorsecondsthrehold")+","+r.getInt("leaveminutesthrehold"))

case class Record_meta( entityid: Int, apmac :String, rssi :Int ,indoorsecondsthrehold :Int, leaveminutesthrehold:Int)
 
val df_meta=hc.createDataFrame(rdd_meta.map(x=> x.split(",")).map( x=> Record_meta(x(0).toInt,x(1),x(2).toInt,x(3).toInt,x(4).toInt))).coalesce(1)

sc.broadcast(df_meta) 


val sql_join="""select distinct raw_data.SourceMac,v_entity_ap_rel.entityid,
v_entity_ap_rel.indoorsecondsthrehold,v_entity_ap_rel.leaveminutesthrehold,
max(unix_timestamp(raw_data.UpdatingTime,'yyyy-MM-dd HH:mm:ss')) as updatingtime
from v_entity_ap_rel ,raw_data 
where raw_data.ApMac=v_entity_ap_rel.apmac
and case when (v_entity_ap_rel.rssi- raw_data.Rssi )>0 then 1 else 0 end=1
group by SourceMac,entityid,indoorsecondsthrehold,leaveminutesthrehold"""


val topics = "topic-ogg"
val ssc = new StreamingContext(sc, Seconds(5))
val topicsSet = topics.split(",").toSet
val kafkaParams = Map[String, String]("metadata.broker.list" -> "192.168.2.53:6667")

val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder]( ssc, kafkaParams, topicsSet)

val lines = messages.map(_._2)

case class Record( ApMac: String, sourcemac :String, rssi :Int ,time :String, UpdatingTime:String)

lines.foreachRDD( rdd=> {
     val df_raw=hc.createDataFrame(rdd.map( x => x.toString.split(",") ).map( x=> Record(x(1).trim,x(2).trim,x(3).toInt,x(4),x(5))))
     df_raw.registerTempTable("raw_data")
     df_meta.registerTempTable("v_entity_ap_rel")
     val df_rows=hc.sql(sql_join).coalesce(1)
     df_rows.foreachPartition { iter =>
       val conf = HBaseConfiguration.create()
       conf.set("hbase.zookeeper.property.clientPort", "2181")
       conf.set("hbase.zookeeper.quorum", "192.168.2.53")
       conf.set("zookeeper.znode.parent", "/hbase-unsecure")
       val  conn = ConnectionFactory.createConnection(conf)
       //val admin = conn.getAdmin();
       val table = conn.getTable(TableName.valueOf("t_indoor_tmp"))
       iter.foreach{ x =>
         val SourceMac= x(0).toString
         val entityid=x(1).toString
         val indoorthrehold=x(2).toString
         val leavethrehold=x(3).toString
         val updatingtime=x(4).toString
         //println("smac",SourceMac,"eid",entityid,"indoorT",indoorthrehold,"LeaveT",leavethrehold,"utime",updatingtime)
         val filterList = new FilterList();
         filterList.addFilter(new PrefixFilter(Bytes.toBytes(SourceMac + '+'.toString + entityid.toString  )))
         val single_done_filter=new SingleColumnValueFilter(Bytes.toBytes("cf"),Bytes.toBytes("done"), CompareFilter.CompareOp.EQUAL,new SubstringComparator("0"))
         single_done_filter.setFilterIfMissing(true)
         filterList.addFilter(single_done_filter)
         val s = new Scan()
         s.setFilter(filterList)
         val scanner = table.getScanner(s)
         val scalaList: List[Result] = scanner.iterator.toList
         var etime=""
         var ltime=""
         //println("scalaList.length=",scalaList.length)
         if (scalaList.length>0 ) {
           //println("there is record not done in hbases ,should be only one record marked as not done")
           for (result <- scalaList) {
             val rowkey=Bytes.toString(result.getRow())
             etime = rowkey.split('+').last
             ltime= Bytes.toString(result.getValue(Bytes.toBytes("cf"),  Bytes.toBytes("ltime")))
             if  (updatingtime.toInt -ltime.toInt >= leavethrehold.toInt  ){
               //update to hbase set done=1 
               val p = new Put(Bytes.toBytes(rowkey))  //rowkey
               p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("done"), Bytes.toBytes("1"))
               table.put(p)
   
               val new_rowkey=SourceMac+ '+'.toString + entityid + '+'.toString + updatingtime.toString
               val p_new = new Put(Bytes.toBytes(new_rowkey));  //rowkey
               p_new.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("done"), Bytes.toBytes("0"))
               p_new.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ltime"), Bytes.toBytes(updatingtime.toString))
               p_new.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("seconds"), Bytes.toBytes("0"))
               table.put(p_new)
   
             } else { // update seconds and leave time 
             val p = new Put(Bytes.toBytes(rowkey));  //rowkey
               p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ltime"), Bytes.toBytes(updatingtime.toString))
               val seconds=updatingtime.toInt - etime.toInt
               p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("seconds"), Bytes.toBytes(seconds.toString))
               table.put(p)
             }
           }
         } else {
           //println("no record in hbase ,we need to insert") //new record for new client macaddress,inset it directly and set done=0 in hbase
           val new_rowkey=SourceMac+ '+'.toString + entityid + '+'.toString + updatingtime.toString
           val p = new Put(Bytes.toBytes(new_rowkey))  //rowkey
           p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ltime"), Bytes.toBytes(updatingtime.toString))
           p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("seconds"), Bytes.toBytes("0"))
           p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("done"), Bytes.toBytes("0"))
           table.put(p)
   
         }
       }
     //after iteration hbase should be closed  
     conn.close()
     }
   })
ssc.start()
ssc.awaitTermination()
