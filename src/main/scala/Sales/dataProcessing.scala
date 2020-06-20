package Sales

//Importing Spark classes

import org.apache.spark.sql.{DataFrame, SparkSession}

object dataProcessing extends App with Serializable {

//  Creating Spark Session
  val spark = SparkSession
    .builder()
    .appName("loader")
    .master("local[3]")
    .getOrCreate()

//  Reading data from HDFS
  val employee = dataLoader("hdfs://localhost:8020/user/hadoop/sqoopimport/employees/")
  val product = dataLoader("hdfs://localhost:8020/user/hadoop/sqoopimport/product/")
  val sales = dataLoader("hdfs://localhost:8020/user/hadoop/sqoopimport/sales/")
  val usersreview = dataLoader("hdfs://localhost:8020/user/hadoop/sqoopimport/usersreview/")

//  Creating TempView on top of SparkDataFrame to work with SQL
  employee.createOrReplaceTempView("Employees")
  product.createOrReplaceTempView("Products")
  sales.createOrReplaceTempView("Sales")
  usersreview.createOrReplaceTempView("UsersReview")

//  SQL Query for Products Report
  val ProductsReport = spark.sql("""select p.Name as ProductName,
                              | count(s.1) as UnitsSold,
                              | count(r.1) as NoOfReviews
                              | from
                              | Products p
                              | join Sales s ON p.`Prod ID` = s.`Prod ID`
                              | join UsersReview r ON s.`Cust Name` = r.`Cust Name` and p.Name = r.`Prod Name`
                              |""".stripMargin)

// Writing Products Report data to a csv in local directory
  dataWriter(ProductsReport,"output/ProductsReport.csv")

  //  SQL Query for Sales Report
  val SalesReport = spark.sql("""select e.Name as EmployeeName,
                                 | p.Name as ProductName,
                                 | sum(s.`Sales Value`) as SalesAmount
                                 | from
                                 | Sales s
                                 | join Employee e ON s.`Emp ID` = e.`Emp ID`
                                 | join Products p ON s.`Prod ID` = p.`Prod ID`
                                 |""".stripMargin)

  // Writing Sales Report data to a csv in local directory
  dataWriter(SalesReport,"output/SalesReport.csv")

spark.stop()

}
