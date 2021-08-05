import org.apache.spark.sql.SparkSession
object problem_scenario1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sparkContext.setLogLevel("WARN")
    //find the total consumers in branch 1
    //spark.sql("select sum(numOfConsumers) from bevboughtall inner join bevbranchall on bevbranchall.beverage = bevboughtall.beverage where bevbranchall.branch = 'Branch1'").show()
    //find the consumers in branch 2
    //spark.sql("select sum(numOfConsumers) from bevboughtall inner join bevbranchall on bevbranchall.beverage = bevboughtall.beverage where bevbranchall.branch = 'Branch2'").show()
  }
}
