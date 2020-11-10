package com.esen

import java.sql.DriverManager

import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object TwentyDataCount {

  def handleMessage2CaseClass(jsonStr: String): KafkaMessage = {
    val gson = new Gson()
    gson.fromJson(jsonStr, classOf[KafkaMessage])
  }


  val reduceFunc = (a: KafkaMessage, b: KafkaMessage) => {
    if(a.insertTime>b.insertTime){
      a
    }else{
      b

    }
  }

  val InsertData=(connstr:String,sql:String) =>{
    //classOf[com.esen.jdbc.PetaBaseDriver]
    classOf[org.apache.hive.jdbc.HiveDriver]
    val conn = DriverManager.getConnection(connstr)
    val stat=conn.createStatement()
    stat.execute(sql)
    stat.close()
    conn.close()

  }

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println(s"""
                            |Usage:TwentyDataCount <brokers> <groupId> <topics>  <connectmater> <tablename>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <groupId> is a consumer group name to consume from topics
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <connection> connect to database
                            |  <tablename> is a table which prepared create in kudu
        """.stripMargin)
      System.exit(1)
    }

    /*      val spark = SparkSession
            .builder
            .appName("StructuredKafkaWordCount")
            .getOrCreate()
          val Array(bootstrapServers, subscribeType, topics) = args
          import spark.implicits._

          val lines = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrapServers)
            .option(subscribeType, topics)
            .load()
            .selectExpr("CAST(value AS STRING)")
            .as[String]
          import org.apache.spark.sql.functions._

          val schema = StructType(List(
            StructField("id", StringType),
            StructField("value", StringType),
            StructField("time", StringType),
            StructField("valueType", StringType),
            StructField("region", StringType),
            StructField("namespace", StringType))
          )
          val df=lines.select(from_json('value.cast("string"), schema) as "value").select($"value.*").toDF()

          df.createOrReplaceTempView("twentysecondscount")*/
    val Array(broker,groupid,topics,connectmater,tablename)=args
    val topiset=topics.split(",").toSet
    val sparkConf=new SparkConf().setAppName("TwentyDataCount")
    val ssc=new StreamingContext(sparkConf,Seconds(5))
    val kafkapararms=Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->broker,
      ConsumerConfig.GROUP_ID_CONFIG->groupid,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG->"latest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer]
    )

    val message=KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topiset,kafkapararms))
    val result=message.map(record=>handleMessage2CaseClass(record.value())).map(line=>(line.mineCodeAndDeviceNo,line)).reduceByKeyAndWindow(reduceFunc,Seconds(25),Seconds(20))
    //val result=message.map(record=>handleMessage2CaseClass(record.value()))
    /* result.foreachRDD(rdd=>{
       val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
       import  spark.implicits._
       val ds=spark.createDataset(rdd)
       ds.createOrReplaceTempView("twentySecondsData")
       spark.sql("select mineCodeAndDeviceNofrom twentySecondsData group by mineCodeAndDeviceNo order by insertTime ")
       ds.groupBy("mineCodeAndDeviceNo").pivot("valueNum").max("valueNum").show()

     })*/
    var timestamp=System.currentTimeMillis()

    val sqlstr=result.map(rdd=>(1,(rdd._2.mineCodeAndDeviceNo,rdd._2.valueNum))).groupByKey().mapValues(rdd=>{
      val idBuffer = new ArrayBuffer[String]()
      val numBuffer = new ArrayBuffer[Double]()
      rdd.foreach(f=>{
        idBuffer+=f._1
        numBuffer+=f._2
      })
      val sql= "insert into "+tablename+ "( time,"+idBuffer.mkString(",")+" )values('"+timestamp+"',"+numBuffer.mkString(",")+")"
      val  connstr="jdbc:hive2://" +connectmater+":21050/default;auth=noSasl"
      InsertData(connstr,sql)
      sql
    })
    sqlstr.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
case class KafkaMessage(id: String, value: String, status: String, realTime: String, fileType: String, fileTime: String,mineCode:String,insertTime:String,deviceCategory:String,deviceNo:String,substationCode:String,mineCodeAndDeviceNo:String,valueNum:Double,deviceName:String,deviceAddr:String)



