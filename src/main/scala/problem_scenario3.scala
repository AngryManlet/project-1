import org.apache.spark.sql.SparkSession

object problem_scenario3 {
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
    //find the drinks in branches 10, 8, or 1
    //spark.sql("select distinct beverage from bevbranchall " +
      //"where branch = 'Branch10' or branch = 'Branch8' or branch = 'Branch1'").show()
    //find the drinks in common between branches 7 and 4
    //spark.sql("select distinct beverage from bevbranchall where branch = 'Branch4' " +
      //"intersect select distinct beverage from bevbranchall where branch = 'Branch7'").show()

  }
}
