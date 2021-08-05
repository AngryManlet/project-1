import org.apache.spark.sql.SparkSession

object problem_scenario5 {
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
    //spark.sql("create table bevbranch1 as (select * from bevbranchall where branch ='Branch1')")
    //spark.sql("alter table bevbranch1 add columns (Notes string, Comments string)")
    spark.sql("select * from bevbranch1").show()
  }
}
