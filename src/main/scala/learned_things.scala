import org.apache.spark.sql.SparkSession

object learned_things {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    //spark.sql("create table mapreducetest as (select * from bevbrancha union select * from bevbranchb union select * from bevbranchc)")
    //spark.sql("drop table mapreducetest")
    //spark.sql("delete from bevbranch1 where beverage = 'Triple_MOCHA'")
  }
}
