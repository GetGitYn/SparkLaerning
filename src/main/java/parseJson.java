import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;


import java.util.*;

public class parseJson {
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

    public static void method1( String[] args) {
                String topic = "t2";
                String timeStampName = args[1];
                String[] colunmList = args[2].split(",");
                String mointorDataName=args[3];
                String tableName=args[4];
               // String connurl=args[5];
                SparkConf sparkConf = new SparkConf().setAppName(topic).setMaster("local[5]");
                JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
                sparkContext.setLogLevel("ERROR");
                JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(20));
                //streamingContext.checkpoint(checkdir);
                Map<String, Object> kafkaParams = new HashMap<String, Object>();
                // Kafka服务监听端口
                kafkaParams.put("bootstrap.servers", "172.21.10.180:6667");
                // 指定kafka输出key的数据类型及编码格式
                kafkaParams.put("key.deserializer", StringDeserializer.class);
                // 指定kafka输出value的数据类型及编码格式
                kafkaParams.put("value.deserializer", StringDeserializer.class);
                // 消费者ID
                kafkaParams.put("group.id", "test");
                // 指定从latest(最新)还是smallest(最早)处开始读取数据
                kafkaParams.put("auto.offset.reset", "latest");
                //kafkaParams.put("enable.auto.commit", true);

                Collection<String> topics = Arrays.asList(topic);
                JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext,
                        LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));
             JavaDStream<Row> dStream= stream.map((Function<ConsumerRecord<String, String>, Row>) s-> {
                            Map<String, Object> hashMap = new HashMap<>();
                            parseBasicjson(hashMap, s.value(), null, false);
                            String keyColumnName = args[0];
                            Object keyColumnValue = null;
                            if (StringUtils.isEmpty(keyColumnName)) {
                                keyColumnValue = hashMap.get("key");
                            } else {
                                keyColumnValue = hashMap.get(keyColumnName);
                            }
                            Object TimeColumnValue = hashMap.get(timeStampName);
                            List<Object> columnLinkedList = new LinkedList<>();
                            columnLinkedList.add(keyColumnValue);
                            columnLinkedList.add(TimeColumnValue);
                            for (String columnName : colunmList) {
                                columnLinkedList.add(hashMap.get(columnName));
                            }
                            /*Seq<Object> columnSeq = JavaConverters.asScalaIteratorConverter(columnLinkedList.iterator()).asScala().toSeq();
                            return  Row.fromSeq(columnSeq);*/
                            return RowFactory.create(columnLinkedList.toArray());
                        });

        ArrayList<StructField> fields = new ArrayList<>();
        StructField field = null;
        field = DataTypes.createStructField(timeStampName, DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("key", DataTypes.StringType, true);
        fields.add(field);
        for (String columnName : colunmList) {
            field = DataTypes.createStructField(columnName, DataTypes.StringType, true);
            fields.add(field);
        }
        field = DataTypes.createStructField(mointorDataName, DataTypes.DoubleType, true);
        fields.add(field);
        StructType schema = DataTypes.createStructType(fields);

        dStream.foreachRDD((rowJavaRDD, time) -> {
         //SparkSession sparkSession= SparkSession.builder().config(sparkConf).getOrCreate();
            SparkSession spark = JavaSparkSessionSingleton.getInstance(rowJavaRDD.context().getConf());
            Dataset<Row> df=spark.createDataFrame(rowJavaRDD,schema);
         df.createOrReplaceTempView("opcdata");
         df.show();
         /*String sql="select * from (select *,row_number() over(partition by key order by "+timeStampName+" desc ) rank  from  opcdata) a where a.rank=1";
         Dataset<Row> ditinctds= spark.sql(sql);
         List<Row> dataList=ditinctds.collectAsList();
         StringBuffer sb=new StringBuffer();
         sb.append("insert into ").append(tableName).append("(").append(timeStampName).append(",");
         for(Row row:dataList){
             String keyName=row.getString(2);
             for (String columnName : colunmList) {
                 sb.append(keyName+"_"+columnName).append(",");
             }
             sb.append(keyName+"_"+mointorDataName).append(",");
         }
         sb.delete(sb.length()-1,sb.length());
         sb.append(")").append("values(").append(System.currentTimeMillis()).append(",");
         for(Row row:dataList){
           for(int i=1;i<=colunmList.length;i++){
               sb.append(row.getString(i+2)).append(",");
           }
           sb.append(row.getDouble(colunmList.length+3)).append(",");
         }
            sb.delete(sb.length()-1,sb.length());
        sb.append(")");
        System.out.println(sb.toString());*/
        });

        streamingContext.start();
        // 等待处理停止
        try {
            streamingContext.awaitTermination();
        } catch (Exception e) {

        }


    }



    public static void main(String[] args) {
     /* String str="{\"_id\":\"5f915d38bfad13eb7f591bfb\",\"value\":\"0.60\",\"status\":\"001\",\"realTime\":\"2020-10-23 15:31:21\",\"fileType\":\"SSSJ\",\"fileTime\":\"20201023153231\",\"mineCode\":\"10017757\",\"insertTime\":1603438326750,\"deviceCategory\":\"analog\",\"deviceNo\":\"001A0103\",\"substationCode\":\"001F\",\"mineCodeAndDeviceNo\":\"10017757001A0103\",\"valueNum\":0.6,\"deviceName\":\"0002\",\"deviceAddr\":\"12煤总回风速\"}";
      Map<String,Object> hashMap=new HashMap<>();
        parseBasicjson(hashMap,str,null,false);
        for(Map.Entry<String,Object> map:hashMap.entrySet()){
            System.out.println(map.getKey()+","+map.getValue());
        }*/
        //SparkSession spark = SparkSession.builder().master("local[3]").appName("opcSpark").getOrCreate();
        //final JavaSparkContext ctx = JavaSparkContext.fromSparkContext(spark.sparkContext());
        String[] aa={"","sourceTime","statusCodeStr","value","aa"};
        method1(aa);

    }
}
