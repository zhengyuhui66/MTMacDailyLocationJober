package mtdailymac

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar
import java.util.regex.Pattern

import com.hiklife.utils.{ByteUtil, HBaseUtil}
import net.sf.json.{JSONArray, JSONObject}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * 每月一号执行一次
  * 功能介绍
  * 统计单位时间(一个月)内Mac采集到的次数，及天数，
  * 依据次数和天数的阈值来判断此Mac经过的devID Location
  */
object App{

  val MACRECORDER_CONF:String="dataAnalysis/mtmacdailylocation.xml"
  val HBASE_SITE_CONF:String="hbase/hbase-site.xml"

  def main(args: Array[String]): Unit = {
    val path=args(0)
    val configUtil = new ConfigUtil(path + MACRECORDER_CONF)
    configUtil.setConfPath(path + HBASE_SITE_CONF);
    val conf = new SparkConf().setAppName(configUtil.appName)
    //conf.setMaster("local")
    val sc=new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
    var hBaseUtil = new HBaseUtil(path + HBASE_SITE_CONF)
    hBaseUtil.deleteTable(configUtil.MTDailyLocation)
    hBaseUtil.deleteTable(configUtil.MTDailyWorkLocation)
    hBaseUtil.deleteTable(configUtil.MTDailyRestLocation)
    //hBaseUtil.deleteTable(configUtil.MTDailyLocationCount)

    hBaseUtil.createTable(configUtil.MTDailyLocation, "DL");
    hBaseUtil.createTable(configUtil.MTDailyWorkLocation, "DL");
    hBaseUtil.createTable(configUtil.MTDailyRestLocation, "DL");
    //hBaseUtil.createTable(configUtil.MTDailyLocationCount, "DL");
    val broadList = sc.broadcast(List(
      configUtil.confPath,configUtil.recoderTable,
      configUtil.MTDailyLocation,configUtil.MTDailyWorkLocation,
      configUtil.MTDailyRestLocation,
      configUtil.connectdays,configUtil.connectcot
    ))
    val start:Calendar=Calendar.getInstance()
    val stop:Calendar=Calendar.getInstance()


    //一个参数则默认执行上个月的统计量
    if(args.length==1){
      start.add(Calendar.MONTH,-1)
      start.set(Calendar.DATE,1)
      start.set(Calendar.HOUR_OF_DAY,0)
      start.set(Calendar.MINUTE,0)
      start.set(Calendar.SECOND,0)
      start.set(Calendar.MILLISECOND,0)

      val lastDay = start.getActualMaximum(Calendar.DAY_OF_MONTH)
      stop.add(Calendar.MONTH,-1)
      stop.set(Calendar.DATE,lastDay)
      stop.set(Calendar.HOUR_OF_DAY,0)
      stop.set(Calendar.MINUTE,0)
      stop.set(Calendar.SECOND,0)
      stop.set(Calendar.MILLISECOND,0)
      //三个参数则执行起始节束时间内的统计量，第二个参数为起始日。第三个参数为节束日
    }else if(args.length==3){
      val pattern=Pattern.compile("^\\d{4}-\\d{1,2}-\\d{1,2}")
      val start_matcher = pattern.matcher(args(1))
      val stop_matcher= pattern.matcher(args(2))
      if(start_matcher.matches()&&stop_matcher.matches()){
        val simple = new SimpleDateFormat("yyyy-MM-dd")
        start.setTime(simple.parse(args(1)))
        stop.setTime(simple.parse(args(2)))
      }else{
        println("时间格式不规范[yyyy-MM-dd HH:mm:ss]")
      }
    }else{
      println("参数不符合规范 Usage: DailyMacJober [generic options] <input dir > " +
        "/n" +
        "[generic options] <input dir > <-m date(yyyy-mm-dd)> <-m date(yyyy-mm-dd)>")
    }
    var DataRDD=query(start,configUtil.confPath,configUtil.recoderDateInx,sc)
    while(start.getTime.before(stop.getTime)){
      start.add(Calendar.DATE,1)
      val DataRDDs =query(start,configUtil.confPath,configUtil.recoderDateInx,sc)
      val temp=DataRDD.union(DataRDDs)
      DataRDD=temp;
    }
    var mDataRDD=DataRDD.flatMap(x=>{
      x._2.listCells()
    }).map(x=>{
      Bytes.toString(CellUtil.cloneValue(x))
    }).mapPartitions(partiton => {
      val conn = ConnectionFactory.createConnection(HBaseUtil.getConfiguration(broadList.value(0).asInstanceOf[String]))
      val macRecorderTable = conn.getTable(TableName.valueOf(broadList.value(1).asInstanceOf[String])).asInstanceOf[HTable]
      macRecorderTable.setAutoFlush(false, false)
      macRecorderTable.setWriteBufferSize(5 * 1024 * 1024)
      val formatStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      partiton.map(x=>{
        val g: Get = new Get(x.getBytes)
        val r: Result = macRecorderTable.get(g)
        val b = r.getValue("RD".getBytes, "IN".getBytes)
        val j: JSONObject = JSONObject.fromObject(new String(b))
        val rt = j.get("rt").toString
        val mac = j.get("mac")
        val ct = j.get("ct").toString
        val devID = j.get("devID")
        val day = ct.substring(0, 10).replace("-", "")
        val sdf=new SimpleDateFormat("HH:mm:ss")
        val tTime=ct.substring(11)
        val nowtime=sdf.parse(tTime)
        val starttime1=sdf.parse("06:00:00")
        val stoptime1=sdf.parse("21:59:59")
        if(CommFunUtils.isEffectiveDate(nowtime,starttime1,stoptime1)){
          (mac + CommFunUtils.SPLIT + devID + CommFunUtils.SPLIT + day, "w")
        }else{
          (mac + CommFunUtils.SPLIT + devID + CommFunUtils.SPLIT + day, "r")
        }
      })
    }).persist(StorageLevel.MEMORY_AND_DISK)

    var nDataRDD=mDataRDD.map(x=>{
      (x._1,1)
    })

    var mRestDataRDD=mDataRDD.filter(x=>{
      if(x._2.equals("r")){
        true
      }else{
        false
      }
    }).map(x=>{
      (x._1,1)
    })

    var mWorkDataRDD=mDataRDD.filter(x=>{
      if(x._2.equals("w")){
        true
      }else{
        false
      }
    }).map(x=>{
      (x._1,1)
    })

    //执行全部统计量
    val caculateResultRDD=exeuteRDD(nDataRDD,broadList)
    //caculateAllRDD(caculateResultRDD,broadList,"ALL")
    exeuteAllRDD(caculateResultRDD,broadList)
    //执行休息时间(晚上时间)统计量
    val caculateRestResultRDD=exeuteRDD(mRestDataRDD,broadList)
    //caculateAllRDD(caculateRestResultRDD,broadList,"REST")
    exeuteRestRDD(caculateRestResultRDD,broadList)
    //执行工作时间(工作时间)统计量
    val caculateWorkResultRDD=exeuteRDD(mWorkDataRDD,broadList)
    //caculateAllRDD(caculateWorkResultRDD,broadList,"WORK")
    exeuteWorkRDD(caculateWorkResultRDD,broadList)
  }

  def exeuteRDD(_DataRDD: RDD[(String,Int)],broadList:Broadcast[List[String]]): RDD[(String,String)] = {

    _DataRDD.reduceByKey(_ + _)
      .map(x => {
        val mKeyTemp = x._1.split(CommFunUtils.SPLIT)
        val mac = mKeyTemp(0)
        val devId = mKeyTemp(1)
        (mac + CommFunUtils.SPLIT + devId, (1, x._2))
      }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).filter(x => {
      val daysThreshold = broadList.value(5).toInt
      val cotsThreshold = broadList.value(6).toInt
      if(x._2._1 > daysThreshold && x._2._2 > cotsThreshold){
        true
      } else {
        false
      }
    }).map(x => {
      val mac_devId = x._1
      val macDevIds = mac_devId.split(CommFunUtils.SPLIT)
      val mac = macDevIds(0)
      val devId = macDevIds(1)
      val days = x._2._1
      val cots = x._2._2
      val nj = new JSONObject()
      val macs=mac.replace("-", "")
      (macs,devId+CommFunUtils.SPLIT+days+CommFunUtils.SPLIT+cots)
    }).groupByKey().map(x => {
      val m = x._2
      val t = new JSONArray();
      val tm: Iterator[String] = m.iterator
      while (tm.hasNext) {
        val value = tm.next()
        val vals = value.split(CommFunUtils.SPLIT)
        val devID = vals(0)
        val days = vals(1)
        val cots = vals(2)
        val nj = new JSONObject()
        nj.accumulate("devId", devID)
        nj.accumulate("day", days)
        nj.accumulate("cot", cots)
        t.add(nj)
      }
      (x._1, t.toString)
    })
  }
/*  def caculateAllRDD(DataRDD: RDD[(String, String)],broadList:Broadcast[List[String]],_type:String): Unit ={
    DataRDD.map(x=>{
      val mv=x._1.substring(0,14)
      (mv,1)
    }).reduceByKey(_+_)
      .foreachPartition(partiton => {
        val conn = ConnectionFactory.createConnection(HBaseUtil.getConfiguration(broadList.value(0).asInstanceOf[String]))
        val mtDailyMacTable = conn.getTable(TableName.valueOf(broadList.value(5).asInstanceOf[String])).asInstanceOf[HTable]
        mtDailyMacTable.setAutoFlush(false, false)
        mtDailyMacTable.setWriteBufferSize(5 * 1024 * 1024)
        partiton.foreach(x => {
          val put = new Put(x._1.getBytes)
          val m=x._2+""

          put.addColumn("DL".getBytes, _type.getBytes, m.getBytes)
          mtDailyMacTable.put(put)
        })
        mtDailyMacTable.flushCommits()
        mtDailyMacTable.close()
        conn.close()
      })
  }*/
  def exeuteAllRDD(DataRDD: RDD[(String, String)],broadList:Broadcast[List[String]]): Unit ={
    DataRDD.foreachPartition(partiton => {
      val conn = ConnectionFactory.createConnection(HBaseUtil.getConfiguration(broadList.value(0).asInstanceOf[String]))
      val mtDailyMacTable = conn.getTable(TableName.valueOf(broadList.value(2).asInstanceOf[String])).asInstanceOf[HTable]
      mtDailyMacTable.setAutoFlush(false, false)
      mtDailyMacTable.setWriteBufferSize(5 * 1024 * 1024)
      partiton.foreach(x => {
        val put = new Put(x._1.getBytes)
        put.addColumn("DL".getBytes, "V".getBytes, x._2.getBytes)
        mtDailyMacTable.put(put)
      })
      mtDailyMacTable.flushCommits()
      mtDailyMacTable.close()
      conn.close()
    })
  }

  def exeuteWorkRDD(DataRDD: RDD[(String, String)],broadList:Broadcast[List[String]]): Unit ={
    DataRDD.foreachPartition(partiton => {
      val conn = ConnectionFactory.createConnection(HBaseUtil.getConfiguration(broadList.value(0).asInstanceOf[String]))
      val mtDailyWorkMacTable = conn.getTable(TableName.valueOf(broadList.value(3).asInstanceOf[String])).asInstanceOf[HTable]
      mtDailyWorkMacTable.setAutoFlush(false, false)
      mtDailyWorkMacTable.setWriteBufferSize(5 * 1024 * 1024)
      partiton.foreach(x => {
        val put = new Put(x._1.getBytes)
        put.addColumn("DL".getBytes, "V".getBytes, x._2.getBytes)
        mtDailyWorkMacTable.put(put)
      })
      mtDailyWorkMacTable.flushCommits()
      mtDailyWorkMacTable.close()
      conn.close()
    })
  }

  def exeuteRestRDD(DataRDD: RDD[(String, String)],broadList:Broadcast[List[String]]): Unit ={
    DataRDD.foreachPartition(partiton => {
      val conn = ConnectionFactory.createConnection(HBaseUtil.getConfiguration(broadList.value(0).asInstanceOf[String]))
      val mtDailyRestMacTable = conn.getTable(TableName.valueOf(broadList.value(4).asInstanceOf[String])).asInstanceOf[HTable]
      mtDailyRestMacTable.setAutoFlush(false, false)
      mtDailyRestMacTable.setWriteBufferSize(5 * 1024 * 1024)
      partiton.foreach(x => {
        val put = new Put(x._1.getBytes)
        put.addColumn("DL".getBytes, "V".getBytes, x._2.getBytes)
        mtDailyRestMacTable.put(put)
      })
      mtDailyRestMacTable.flushCommits()
      mtDailyRestMacTable.close()
      conn.close()
    })
  }

  /**
    * 生成HBASE RDD
    *
    * @param epc
    * @param hbasepath
    * @param tableName
    * @param sc
    * @return
    */
  def query(_mTime:Calendar,hbasepath: String, tableName: String, sc: SparkContext): RDD[(ImmutableBytesWritable, Result)] ={
    val mTime=Calendar.getInstance()
    mTime.setTime(_mTime.getTime)
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("RD"))
    scan.addColumn(Bytes.toBytes("RD"),Bytes.toBytes("IN"))
    mTime.set(Calendar.HOUR_OF_DAY,23)
    mTime.set(Calendar.MINUTE,59)
    mTime.set(Calendar.SECOND,59)
    val formatStr:SimpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val rowkey=getPrex(formatStr.format(mTime.getTime))
    val startrowkey=rowkey+getPrexTime(formatStr.format(mTime.getTime))
    scan.setStartRow(startrowkey.getBytes)
    mTime.add(Calendar.DATE,-1)
    val stoprowkey=rowkey+getPrexTime(formatStr.format(mTime.getTime))
    scan.setStopRow(stoprowkey.getBytes)
    val scans = new util.ArrayList[Scan]()
    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray)
    val hconf = HBaseConfiguration.create()
    hconf.addResource(new Path(hbasepath))
    hconf.set(TableInputFormat.INPUT_TABLE, tableName)
    hconf.set(TableInputFormat.SCAN, ScanToString)
    val hBaseRDD = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    hBaseRDD
  }

  def getPrex(datetime:String):String={
    val dateTimes=datetime.substring(0,10).replace("-","")
    val result=CommFunUtils.byte2HexStr(CommFunUtils.GetHashCodeWithLimit(dateTimes, 0xFF).toByte)
    result.toUpperCase
  }

  def getPrexTime(datetime:String):String={
    val bb = new Array[Byte](4)
    ByteUtil.putInt(bb, ("2524608000".toLong - CommFunUtils.Str2Date(datetime).getTime / 1000).asInstanceOf[Int], 0)
    CommFunUtils.byte2HexStr(bb)
  }
}
