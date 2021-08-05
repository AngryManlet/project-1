import org.apache.spark.sql.SparkSession
object problem_scenario2 {
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
    //spark.sql("create table bevNumSum as (select bevboughtall.beverage, sum(numOfConsumers) as totalBought from bevboughtall group by beverage)")
    //spark.sql("select * from bevNumSum order by totalBought desc").show()
    //find the most bought drink in branch 1
    //spark.sql("select bevbranchall.beverage, bevnumsum.totalBought" +
      //" from bevbranchall join bevnumsum on bevbranchall.beverage = bevnumsum.beverage" +
      //" where bevbranchall.branch = 'Branch1' order by bevnumsum.totalbought desc limit 1").show()
    //find the least bought drink in branch 2
    //spark.sql("select bevbranchall.beverage, bevnumsum.totalBought" +
      //" from bevbranchall join bevnumsum on bevbranchall.beverage = bevnumsum.beverage" +
      //" where bevbranchall.branch = 'Branch2' order by bevnumsum.totalbought asc limit 1").show()
    //find the average number of drinks bought
    //spark.sql("select first(bevbranchall.beverage), avg(bevnumsum.totalBought) as average_drink" +
      //" from bevbranchall join bevnumsum on bevbranchall.beverage = bevnumsum.beverage" +
      //" where bevbranchall.branch = 'Branch2'").show()
  }
}
