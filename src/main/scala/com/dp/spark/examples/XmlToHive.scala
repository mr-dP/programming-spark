package com.dp.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object XmlToHive {

  //  we will read the dataset which is currently in XML format using Spark
  //  we will create a Hive table to load the dataset into the Hive table using Parquet file format
  //  we will also partition the data while loading the Hive table

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      logger.error("Usage: XmlToHive file_name hive_table_name spark_warehouse_dir")
      System.exit(1)
    }

    logger.info("Starting XmlToHive")
    val spark = createSparkSession(args(2))

    val xml_raw = readXmlFile(spark, args(0))

    val df_with_new_colNames = transformColumnNames(xml_raw)

    //    df_with_new_cols.show(false)

    val df_with_age_group = defineAgeGroups(df_with_new_colNames)

    writeToHiveTable(df_with_age_group, args(1))

    logger.info("Finishing XmlToHive")

  }

  def createSparkSession(sparkWarehouseDir: String): SparkSession = {
    SparkSession.builder()
      .config("spark.sql.warehouse.dir", sparkWarehouseDir) //    this is the hive's warehouse directory. this is where data behind the Hive's table is stored in hdfs
      .config("hive.exec.dynamic.partition", "true") //    we will be creating partitions in Hive on-the-fly
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport() //    enable SparkSession to work with Hive
      .getOrCreate()
  }

  def readXmlFile(spark: SparkSession, file: String): DataFrame = {
    spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "row") //    helps Spark identify how to look for individual record in the XML file
      .load(file)
  }

  def transformColumnNames(xml_raw: DataFrame): DataFrame = {

    val colNames = xml_raw.columns
      .map(_.replace("_", ""))
      .map(colName =>
        "_?[A-Z][a-z\\d]+".r.findAllMatchIn(colName)
          .map(_.group(0).toLowerCase)
          .mkString("_")
      )

    xml_raw.toDF(colNames: _*)
  }

  def defineAgeGroups(df_with_new_cols: DataFrame): DataFrame = {
    df_with_new_cols
      .withColumn("age_group", when(col("age") < 20, "group_juniors")
        .when(col("age") >= 20 and col("age") < 30, "group_20s")
        .when(col("age") >= 30 and col("age") < 40, "group_30s")
        .when(col("age") >= 40 and col("age") < 50, "group_40s")
        .when(col("age") >= 50 and col("age") < 60, "group_50s")
        .when(col("age") >= 60, "group_seniors")
      )
  }

  def writeToHiveTable(df: DataFrame, tableName: String): Unit = {
    df.write
      .format("parquet")
      .partitionBy("age_group")
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName)
  }

}
