import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * 
 *@author yin
 *@since 2020年8月7日
 */
public class JavaSparkSessionSingleton {

	 private static transient SparkSession instance = null;
	  public static SparkSession getInstance(SparkConf sparkConf) {
	    if (instance == null) {
	      instance = SparkSession
	        .builder()
	        .config(sparkConf)
	        .getOrCreate();
	    }
	    return instance;
	  }
	
	
}
