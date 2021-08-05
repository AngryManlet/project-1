import org.apache.spark.sql.SparkSession

object problem_scenario4 {
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
    //create partitions in the data
    //spark.sql("select distinct beverage, sum(numOfConsumers) over(partition by beverage) as totalConsumers " +
      //"from bevboughtall").show()
    //create a view in the data
    //spark.sql("create view testview as select branch, beverage from bevbranchall where branch = 'Branch1'")
    //spark.sql("select * from testview").show()
  }
}
