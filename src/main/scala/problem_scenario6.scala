import org.apache.spark.sql.SparkSession

object problem_scenario6 {
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
    //spark.sql("create table deletedrow as (select * from bevbranch1 where not beverage = 'MED_LATTE')")
    //spark.sql("select * from deletedrow").show()
    //spark.sql("truncate table bevbranch1")
    //spark.sql("insert into bevbranch1 select * from deletedrow")
    //spark.sql("select * from bevbranch1").show()
    //spark.sql("drop table deletedrow")
  }
}
