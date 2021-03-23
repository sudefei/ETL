package com.utils

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext, SaveMode}
import org.slf4j.LoggerFactory

/**
  * @Author defei.su
  * @Date 2021/1/29 15:04
  */
object CZS_Utils {


//  private val logger = LoggerFactory.getLogger(JYK_CZS.getClass)
  val zkUrl = "master.cm.com,master2.cm.com,node1.cm.com:2181"

  val prop = {
    val p = new java.util.Properties()
    p.put("driver", "oracle.jdbc.OracleDriver")
    p.put("user", "PDCZS")
    p.put("password", "password123456")
    p
  }

  def getSampleScore(sQLContext: SQLContext, sampleSocre: DataFrame): DataFrame = {
    val makeDt = udf((x: String) => {
      if (x == "合格      ") {
        "QLF"
      } else {
        "DIS_QLF"
      }
    })
    val sample_code = sampleSocre.withColumn("INSRESULTCODE", makeDt(sampleSocre("PACK_ROLL_JUDGE_NAME")))
    sample_code
  }


  def getPhoenixReader(sQLContext: SQLContext, tableName: String): DataFrameReader = {
    // 2.使用 phoenix-spark 直接连接Phoenix集群 (org.apache.phoenix.spark)
    sQLContext.read
      .format("org.apache.phoenix.spark")
      .options(Map("table" -> tableName, "zkUrl" -> zkUrl))
  }

  def getPhoenixDF(sQLContext: SQLContext, tableName: String): DataFrame ={
       val df = getPhoenixReader(sQLContext,tableName).load()
       df
  }

  def createPhoenixRegisterTable(sQLContext: SQLContext, tableName: String): Unit = {
    val rdf = sQLContext.load("org.apache.phoenix.spark", Map("table" -> tableName, "zkUrl" -> zkUrl))
    println(tableName.substring(7))
    rdf.registerTempTable(tableName.substring(7))
  }

  def getCZSOracleDF(sQLContext: SQLContext, tableName: String): DataFrame = {
    val df = sQLContext.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@//10.10.184.150/pdczs.yc.sh.tb")
      .option("driver", "oracle.jdbc.OracleDriver")
      .option("dbtable", tableName)
      .option("user", "PDCZS")
      .option("password", "password123456")
      .load()
    df
  }

  def getSPCOracleDF(sQLContext: SQLContext, tableName: String): DataFrame = {
    val df = sQLContext.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@//10.10.184.150/PDJBSPC.yc.sh.tb")
      .option("driver", "oracle.jdbc.OracleDriver")
      .option("dbtable", tableName)
      .option("user", "PDJBSPC")
      .option("password", "password123456")
      .load()
    df
  }



    def writeOracleTable(dataFrame: DataFrame, tableName: String, saveMode: SaveMode): Unit = {
    dataFrame
      .dropDuplicates(Seq("ID"))
      .repartition(1)
      .write.mode(SaveMode.Append).jdbc("jdbc:oracle:thin:@//10.10.184.150/PDCZS", tableName, prop)
//    logger.warn(s"落地oracle表${tableName}成功")
  }

  def writeOracleTableWithoutDrop(dataFrame: DataFrame, tableName: String, saveMode: SaveMode): Unit = {
    dataFrame
      .repartition(1)
      .write.mode(SaveMode.Append).jdbc("jdbc:oracle:thin:@//10.10.184.150/PDCZS", tableName, prop)
//    logger.warn(s"落地oracle表${tableName}成功")
  }


  def DfToHdfs(sc:SparkContext,dataFrame:DataFrame,tableName:String,system:String): Unit ={
    // 处理结果写出到Hive
    val hdfsConf = sc.hadoopConfiguration
    val hdfs = FileSystem.get(new URI("/"), hdfsConf, "hdfs")
    val path = new Path(s"/user/hive/warehouse/pd_czs.db/${tableName.split("\\.")(1).toLowerCase()}_${system.toLowerCase()}")
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)
    }
    // 删除DF输出时的括号
    // 本质就是直接将每条记录转化为一个字符串然后删除两侧的括号
    dataFrame.map(row => {
      val str = row.toString
      str.substring(1, str.length() - 1).replaceAll("null", "")
    }).repartition(10).saveAsTextFile(s"/user/hive/warehouse/pd_czs.db/${tableName.split("\\.")(1).toLowerCase()}_${system.toLowerCase()}")
  }

   def createDfWithSchema(sQLContext: SQLContext,tableName:String,originDF :DataFrame,system:String): DataFrame ={
//     logger.warn(s"final_join_CZS_MulAnalysisOfPhysical 的schema ：${originDF.schema}")

     val origin_schema = getCZSOracleDF(sQLContext, tableName).schema
     println("=====================>   CZS 源表的Schema：")
     getCZSOracleDF(sQLContext, tableName).printSchema()
     println("=====================>   生成表的 Schema：")
     originDF.printSchema()
//     logger.warn(s"origin_oracle_CZS_MulAnalysisOfPhysical schema:$origin_schema")
     val targetDF = sQLContext.createDataFrame(originDF.rdd, origin_schema)
     targetDF.show(10)
     targetDF
   }

}
