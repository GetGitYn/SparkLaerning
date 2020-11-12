package com.esen;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import scala.Tuple2;

public class parseJson1 {
    private static Connection conn;
    public static synchronized Connection getConn(String url) throws SQLException {
       if(conn==null){
           try {
               Class.forName("org.apache.hive.jdbc.HiveDriver");
               conn = DriverManager.getConnection(url);
           } catch (ClassNotFoundException e) {
               e.printStackTrace();
           } catch (SQLException e) {
               e.printStackTrace();
           }
       }
        return conn;
    }
    public static void parseBasicjson(Map<String, Object> map, String jsonStr, String parent_name, boolean isArray) {
        try {
            map.put("key",parent_name);
            if (null != jsonStr && !"".equals(jsonStr)
                    && jsonStr.length() > 1) {
                if (isArray) {
                    JSONArray reqJson = new JSONArray(jsonStr);
                    StringBuffer sb = new StringBuffer();

                    for (int i = 0; i < reqJson.length(); i++) {
                        Object value = reqJson.get(i);
                        // 1.数组里面是对象
                        if (value instanceof JSONObject) {
                            parseBasicjson(map, value.toString(), parent_name, false);
                        } else if (value instanceof JSONArray) {
                            // 1.数组里面是数组
                            parseBasicjson(map, value.toString(), parent_name, true);
                        } else {
                            // 1.数组里面是字符串
                            if (sb.length() > 0)
                                sb.append(",");
                            sb.append(value.toString().trim());
                            if (i == reqJson.length() - 1) {
                                map.put(parent_name, sb.toString());
                            }
                        }
                    }
                } else {
                    JSONObject reqJson = new JSONObject(jsonStr);
                    Iterator<?> keys = reqJson.keys();
                    StringBuffer sb = new StringBuffer();

                    while (keys.hasNext()) {
                        String key = keys.next().toString();
                        Object value = reqJson.get(key);
                        sb.delete(0, sb.length());
                        if (null != parent_name && !"".equals(parent_name)) {
                            // sb.append(parent_name + "_" + key);
                            sb.append(key);
                        } else {
                            sb.append(key);
                        }
                        if (value instanceof JSONObject) {
                            parseBasicjson(map, value.toString(), sb.toString(), false);
                        } else if (value instanceof JSONArray) {
                            parseBasicjson(map, value.toString(), sb.toString(), true);
                        } else {
                            map.put(sb.toString(), value.toString().trim());
                        }
                    }
                }

            }
        } catch (Exception e) {
        }
    }

    public static void method1( String[] args,String keyColoumn) {
        String timeStampName = args[0];
        String[] colunmList = args[1].split(",");
        String mointorDataName=args[2];
        String tableName=args[3];
        String brokers=args[4];
        String groups=args[5];
        String topic=args[6];
        String connurl=args[7];
        String keyColumnName = keyColoumn;
        SparkConf sparkConf = new SparkConf().setAppName(topic);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("ERROR");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(5));
        SparkSession spark= SparkSession.builder().config(sparkConf).getOrCreate();
        //streamingContext.checkpoint(checkdir);
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        // Kafka服务监听端口
        kafkaParams.put("bootstrap.servers", brokers);
        // 指定kafka输出key的数据类型及编码格式
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        // 指定kafka输出value的数据类型及编码格式
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        // 消费者ID
        kafkaParams.put("group.id",groups);
        // 指定从latest(最新)还是smallest(最早)处开始读取数据
        kafkaParams.put("auto.offset.reset", "latest");
        //kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList(topic.split(","));
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext,
                LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

        JavaDStream<Row> dStream= stream.map((Function<ConsumerRecord<String, String>, Row>) s-> {
            Map<String, Object> hashMap = new HashMap<>();
            parseBasicjson(hashMap, s.value(), null, false);

            Object keyColumnValue = hashMap.get(keyColumnName);
            Object TimeColumnValue = hashMap.get(timeStampName);
            List<Object> columnLinkedList = new LinkedList<>();
            columnLinkedList.add(TimeColumnValue.toString());
            columnLinkedList.add(keyColumnValue);
            for (String columnName : colunmList) {
                columnLinkedList.add(hashMap.get(columnName));
            }
            columnLinkedList.add(hashMap.get(mointorDataName));
            //columnLinkedList.add(Integer.parseInt(hashMap.get(mointorDataName).toString()));
           //columnLinkedList.forEach(e->System.out.println(e));
            return RowFactory.create(columnLinkedList.toArray());
        });

        JavaPairDStream<String,Row> dStreamWindow=dStream.mapToPair((PairFunction<Row, String, Row>)s->new Tuple2<>(s.getString(1),s))
                .reduceByKeyAndWindow(new Function2<Row, Row, Row>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Row call(Row arg0, Row arg1) throws Exception {
                        // TODO Auto-generated method stub
                        return Long.parseLong(arg0.getString(0))>Long.parseLong(arg1.getString(0))?arg0:arg1;
                    }
                },Durations.seconds(25), Durations.seconds(20));


        dStreamWindow.repartition(1).foreachRDD((VoidFunction2<JavaPairRDD<String, Row>, Time>)(rdd,time)->{

            Map<String,Row> map=rdd.collectAsMap();
            StringBuffer sb=new StringBuffer();
            sb.append("insert into ").append(tableName).append("(").append(timeStampName).append(",");
            for(String keyName:map.keySet()){
                for (String columnName : colunmList) {
                    sb.append(keyName.replace(".","_")+"_"+columnName).append(",");
                }
                sb.append(keyName.replace(".","_")+"_"+mointorDataName).append(",");
            }
            sb.delete(sb.length()-1,sb.length());
            sb.append(")").append("values(").append("'").append(time).append("'").append(",");
            for(Entry<String, Row> entry:map.entrySet()) {
                Row row=entry.getValue();
                for(int i=1;i<=colunmList.length;i++){
                    sb.append("'").append(row.getString(i+1)).append("'").append(",");
                }
                sb.append(row.get(colunmList.length+2)).append(",");
            }
            sb.delete(sb.length()-1,sb.length());
            sb.append(")");
            if(!sb.toString().isEmpty()) {
                conn = getConn(connurl);
                Statement stat = conn.createStatement();
                try {
                    stat.executeUpdate(sb.toString());
                } finally {
                    stat.close();
                }
            }

        } );


        //使用df 查询数据，但是不知道为啥spark.sql用不了，
      /* ArrayList<StructField> fields = new ArrayList<>();
        StructField field = null;
        field = DataTypes.createStructField(timeStampName, DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("key", DataTypes.StringType, true);
        fields.add(field);
        for (String columnName : colunmList) {
            field = DataTypes.createStructField(columnName, DataTypes.StringType, true);
            fields.add(field);
        }
        field = DataTypes.createStructField(mointorDataName, DataTypes.IntegerType, true);
        fields.add(field);
        StructType schema = DataTypes.createStructType(fields);
        dStreamWindow.print();*/


       /* dStreamWindow.foreachRDD((rowJavaRDD, time) -> {

           // SparkSession spark = JavaSparkSessionSingleton.getInstance(rowJavaRDD.context().getConf());
            Dataset<Row> df=spark.createDataFrame(rowJavaRDD,schema);
         //df.createOrReplaceTempView("opcdata");
         df.show();
         //df.joinWith(dtime, condition)
        // spark.sql("select key,sourceTime,value from opcdata ").show();
        // df.select("key").show();
        //String sql="select * from (select *,row_number() over(partition by key order by "+timeStampName+" desc ) rank  from  opcdata) a where a.rank=1";
         //Dataset<Row> ditinctds= spark.sql(sql);
         List<Row> dataList=df.collectAsList();
         StringBuffer sb=new StringBuffer();
         sb.append("insert into ").append(tableName).append("(").append(timeStampName).append(",");
         for(Row row:dataList){
             String keyName=row.getString(1);
             for (String columnName : colunmList) {
                 sb.append(keyName+"_"+columnName).append(",");
             }
             sb.append(keyName+"_"+mointorDataName).append(",");
         }
         sb.delete(sb.length()-1,sb.length());
         sb.append(")").append("values(").append("'").append(time).append("'").append(",");
         for(Row row:dataList){
           for(int i=1;i<=colunmList.length;i++){
               sb.append("'").append(row.getString(i+1)).append("'").append(",");
           }
           sb.append(row.getInt(colunmList.length+2)).append(",");
         }
            sb.delete(sb.length()-1,sb.length());
        sb.append(")");
        System.out.println(sb.toString());
        });*/


        streamingContext.start();
        // 等待处理停止
        try {
            streamingContext.awaitTermination();
        } catch (Exception e) {

        }


    }

    public static void main(String[] args) {
        if(args.length<8) {
            String usage = "Usage:TwentyDataCount keyColumn timeColum selectedColumn dataColumn tablename brokers groupId topics  connectmater \n"
                    + "<timeColumn>  is record time\r\n"
                    + "<selectedColumn> other column into petabase ,use  to concat columname;such as a,b\r\n"
                    + "<dataColumn> column represent moniator data \r\n "
                    + "<tablename> petabase tablaname\r\n"
                    + "<brokers> is a list of one or more Kafka brokers\r\n" +
                    " <groupId> is a consumer group name to consume from topics\r\n" +
                    " <topics> is a list of one or more kafka topics to consume from\r\n" +
                    " <connection> connect to database\r\n"
                    +"<keyColumn>  key to identify record ,such as mineCodeAndDeviceNo:10, mineCodeAndDeviceNo:20 "
                    + "if keyName not exists  ,such as key:t1:{},t2:{} ;make it  empty\n";
           System.err.println(usage);
            System.exit(1);
        }
      // String[] args1={"insertTime","deviceAddr,status","valueNum","opchttp","172.21.10.180:6667","test","t2","jdbc:hive2://172.21.10.179:21050/test;auth=noSasl","mineCodeAndDeviceNo"};

       if(args.length==8) {
           method1(args,"key");
       }else if (args.length==9){
           method1(args,args[8]);
       }else{
           System.err.println("error parameter number ,for zero parameter to show usage");
           System.exit(1);
       }

    }
}
