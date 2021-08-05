import org.apache.spark.sql.SparkSession
object create_tables {
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
    //spark.sql("create table bevBranchA(beverage String,branch String) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE bevBranchA")
    //spark.sql("SELECT * FROM bevBranchA").show()
    //spark.sql("create table bevBranchB(beverage String,branch String) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE bevBranchB")
    //spark.sql("SELECT * FROM bevBranchB").show()
    //spark.sql("create table bevBranchC(beverage String,branch String) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE bevBranchC")
    //spark.sql("SELECT * FROM bevBranchC").show()
    //spark.sql("create table bevBoughtA(beverage String,numOfConsumers Int) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE bevBoughtA")
    //spark.sql("SELECT * FROM bevBoughtA").show()
    //spark.sql("create table bevBoughtB(beverage String,numOfConsumers Int) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE bevBoughtB")
    //spark.sql("SELECT * FROM bevBoughtB").show()
    //spark.sql("create table bevBoughtC(beverage String,numOfConsumers Int) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE bevBoughtC")
    //spark.sql("SELECT * FROM bevBoughtC").show()
    //spark.sql("create table bevBranchAll as (select * from bevbrancha union select * from bevbranchb union select * from bevbranchc)")
    //spark.sql("select * from bevBranchAll").show()
    //spark.sql("create table bevBoughtAll as (select * from bevboughta union select * from bevboughtb union select * from bevboughtc)")
    //spark.sql("select * from bevBoughtAll").show()
    //spark.sql("select * from bevnumsum").show()
  }
}
