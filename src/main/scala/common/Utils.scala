package common
import org.apache.spark.sql.SparkSession


/**
  * Created by jnayak on 11/11/2019.
  */

// Initialise spark Session
object Utils {
  val spark = SparkSession.builder
    .master("local[2]")
    .appName("ImdbAnalytics")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
}
