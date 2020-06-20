// this can be called to write data to csv

package Sales

import org.apache.spark.sql.{DataFrame, SparkSession}

object dataWriter extends App with Serializable {

  def apply(Table: DataFrame, filepath: String) = {

    val spark = SparkSession
      .builder()
      .appName("loader")
      .master("local[3]")
      .getOrCreate()

    Table.coalesce(1).write.format("csv")
      .option("header", true)
      .mode("overwrite")
      .save(filepath)


  }
}
