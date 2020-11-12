package sparkrdd

/**
  * 低级api:rdd,共享变量或者累加器
  * 入口：SparkContext
  * rdd:只读不可变的，且已经分块的，可被并行处理的，记录集合 rdd存放对象
  * 优点：可以完全控制，任何格式存储任何内容
  * 缺点：无论什么任务都得从底层开始，需要自己写优化代码
  * 即：除非有非常明确的理由，不然不要创建rdd 如：自定义分区
  * 常用rdd类型：通用型，k-v rdd
  * rdd 5个主要内部属性：数据分片列表，作用在每个数据分片的计算函数，
  * 与其他rdd依赖，k-v类型rdd分片方法，分片位置处理优先级
  *
  * testFile  读取文件，将文件内容转为rdd,文件每行一个row 对象  paramter:文件路径
  * wholeTextFiles  读取文件转成rdd,rdd 内容为 <文件名对象，字符串对象(文件内容)> paramter:文件路径
  *  filter  根据特定条件过滤数据集，true or false    paramter: f: T => Boolean
  *  distinct 去重  无参
  *   map 对输入数据集中的每一条记录执行转换方法返回一个新的rdd  paramter:f: T => U
  *   flatmap 返回一个 TraversableOnce 对象，`Iterator` and `Traversable 公共特性和方法 paramter:f: T => TraversableOnce[U]
  *   sortBy  获取到rdd中的某个值，对该值进行排序，返回对该值排序后的rdd   paramter:  f: (T) => K ,排序key 方法，升序还是降序，分区
  *   默认升序，创建一个k,v 二元组之后进行排序 分区好像没区别
  *   randomsplit 将一个rdd 切分为多个，返回一个包含rdd的数组
  *
  *   action 算子：
  *   reduce : 将rdd 中任何类型的值规约为一个值 参数： f(a T,b T)=>T
  *   count : 根据某种函数计算符合条件的数据个数 paramter:f(a A)=>boolean
  *   countByValue
  *
  *
  *
  *
  *
  *
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object rddlearn1 {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setAppName("rddlearn1").setMaster("local[3]")
    val sparkContext=new SparkContext(sparkconf)
    val sparkSession=SparkSession.builder().config(sparkconf).getOrCreate()
    import sparkSession.implicits._
    sparkSession.range(10).toDF().rdd.map(row=>row.getLong(0)).collect()
    sparkSession.range(10).rdd.flatMap(a=>Seq.apply(a*10)).foreach(a=>{print(a.toString);printf("aaa")})
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    sparkContext.wholeTextFiles("G:\\learningproject\\data\\bike-data\\201508_station_data.csv").foreach(s=>printf(s._1.concat(s._2.toString)))
    sparkContext.parallelize(myCollection,2).sortBy(s=>s).collect().foreach(s=>print(s))
    sparkContext.parallelize(myCollection,1).sortBy(s=>s).collect().foreach(s=>print(s))
   val testrdd=sparkContext.parallelize(myCollection,1);
    testrdd.reduce((a,b)=>a.concat(b)).count(a=>a.isDigit)

  }


}
