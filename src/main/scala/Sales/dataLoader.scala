// this can be called to read data from csv

package Sales

import org.apache.spark.sql.{DataFrame, SparkSession}

object dataLoader extends App with Serializable {

  def apply(filepath:String) = {

    val spark = SparkSession
      .builder()
      .appName("loader")
      .master("local[3]")
      .getOrCreate()

    val data = spark.read.format("csv").option("header", true).option("inferSchema", true).load(filepath)

    data

  }

}
