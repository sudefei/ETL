package com.wisdom

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import com.utils.CZS_Utils
import org.apache.avro.generic.GenericData
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * @Author defei.su
  * @Date 2021/01/29 16:24
  */
object SPC_CZS {
  private val logger = LoggerFactory.getLogger(SPC_CZS.getClass)

  val conf = new SparkConf().setAppName("SPC_CZS")
      .setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  sc.setLogLevel("WARN")

  val system = "SPC"

  val zkUrl = "master.cm.com,master2.cm.com,node1.cm.com:2181"
  //Phoenix MES TABLE
  val TABLE_MES_BAS_FINISHEDMAT = "PD_KQMES.BAS_FINISHEDMAT"
  val TABLE_MES_BAS_WKS_PRODUCTIONWORKORDER= "PD_KQMES_PLN.WKS_PRODUCTIONWORKORDER"
  val TABLE_MES_WKP_ONWORK = "PD_KQMES_WKS.WKP_ONWORK"
  val TABLE_MES_WKP_ONWORK_DET = "PD_KQMES_WKS.WKP_ONWORK_DET"

  //Oracle SPC TABLE
  val TABLE_SPC_DETAIL = "PDJBSPC.CORE_SPC_DETAIL"
  val TABLE_SPC_MAIN = "PDJBSPC.CORE_SPC_MAIN"
  val TABLE_SPC_WORKORDER = "PDJBSPC.CORE_SPCWORKORDER"
  val TABLE_SPC_BAS_TAG = "PDJBSPC.BAS_TAG"

  // Oracle CZS TABLE
  val TABLE_CZS_MULANALYSISOFPHYSICAL="PDCZS.CZS_MULANALYSISOFPHYSICAL"



  val prop = {
    val p = new java.util.Properties()
    p.put("driver", "oracle.jdbc.OracleDriver")
    p.put("user", "PDJBSPC")
    p.put("password", "password123456")
    p
  }


  def main(args: Array[String]): Unit = {
    val arg1 = args(0)
    val arg2 = args(1)
    var seconds = 0
//    var seconds = 3600 * 24
    if (arg2 == "hour") {
      seconds = arg1.toInt * 3600
    } else if (arg2 == "day") {
      seconds = arg1.toInt * 24 * 3600
    } else {
      throw new Exception("参数格式错误：'" + arg2 + "'")
    }

    //===============>>>>   获取Phonenix 连接，注册表,并进行ETL获取数据

    val oracleDialect = new JdbcDialect {
      override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle") || url.contains("oracle")

      //getJDBCType is used when writing to a JDBC table
      override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
        case StringType => Some(JdbcType("VARCHAR2(36)", java.sql.Types.VARCHAR))
        case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.NUMERIC))
        case IntegerType => Some(JdbcType("NUMBER(16)", java.sql.Types.NUMERIC))
        case LongType => Some(JdbcType("NUMBER(16)", java.sql.Types.NUMERIC))
        case DoubleType => Some(JdbcType("NUMBER(19,5)", java.sql.Types.NUMERIC))
        case FloatType => Some(JdbcType("NUMBER(19,5)", java.sql.Types.NUMERIC))
        case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.NUMERIC))
        case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.NUMERIC))
        case BinaryType => Some(JdbcType("BLOB", java.sql.Types.BLOB))
        case TimestampType => Some(JdbcType("DATE", java.sql.Types.DATE))
        case DateType => Some(JdbcType("DATE", java.sql.Types.DATE))
        case _ => None
      }
    }
    JdbcDialects.registerDialect(oracleDialect) //转换为oracle的数据格式

    val wkp = getMESWkp(sqlContext,TABLE_MES_WKP_ONWORK,TABLE_MES_WKP_ONWORK_DET)
    val wks = getMESWksProduct(sqlContext,TABLE_MES_BAS_WKS_PRODUCTIONWORKORDER,TABLE_MES_BAS_FINISHEDMAT)
    val detail = getSPCDetail(sqlContext,TABLE_SPC_DETAIL,TABLE_SPC_WORKORDER,TABLE_SPC_BAS_TAG,seconds)
    // =============>  获取最终大表
    logger.warn(s"开始获取最终大表 ${TABLE_CZS_MULANALYSISOFPHYSICAL}")
    val final_detail = getFinalTable(detail,wkp,wks)

    // =============>   获取OracleSchema 创建 DF
    val final_df = CZS_Utils.createDfWithSchema(sqlContext,TABLE_CZS_MULANALYSISOFPHYSICAL,final_detail,system)

    // =============>    将DF写到HDFS上
    logger.warn("开始将数据写入HDFS")
    CZS_Utils.DfToHdfs(sc,final_df,TABLE_CZS_MULANALYSISOFPHYSICAL,system)
    logger.warn(s"落地HDFS成功，数据条数为:${final_df.count()}")
  }


  def getMESWkp(sQLContext: SQLContext,wkp_onwork : String, wkp_onwork_det:String): DataFrame ={
    val wkp_onwork_df = CZS_Utils.getPhoenixDF(sQLContext,wkp_onwork)
    val wkp_onwork_det_origin_df = CZS_Utils.getPhoenixDF(sQLContext,wkp_onwork_det)
    val wkp_onwork_det_df = wkp_onwork_det_origin_df.select(wkp_onwork_det_origin_df("MAINID"),wkp_onwork_det_origin_df("STAFFNAME"))
    val wkp = wkp_onwork_df.join(wkp_onwork_det_df,wkp_onwork_df("ID") === wkp_onwork_det_df("MAINID") ,"left_outer" )
    wkp
  }

  def getMESWksProduct(sQLContext: SQLContext,wks_product : String, bas_finishedmat:String): DataFrame ={
    val wks_product_df = CZS_Utils.getPhoenixDF(sQLContext,wks_product)
    val bas_finishedmat_origin_df = CZS_Utils.getPhoenixDF(sQLContext,bas_finishedmat)
    val bas_finishedmat_df= bas_finishedmat_origin_df.select(bas_finishedmat_origin_df("MATCODE"),bas_finishedmat_origin_df("SYSTANDARDCODE"))
    val wks = wks_product_df.join(bas_finishedmat_df,wks_product_df("MATERIALCODE") === bas_finishedmat_df("MATCODE") ,"left_outer" )
//    wks.show(5)
    wks
  }


  def getSPCDetail(sQLContext: SQLContext,spc_detail : String, spc_workorder:String,spc_bas_tag:String ,seconds:Int): DataFrame ={

    val accWorkFlagCode = udf((work_status_name: String) => {
      work_status_name match {
        case "正常" => "A"
        case "轮保" => "B"
        case "测试" => "C"
        case _ => null
      }
    })

    val accInsResultCode = udf((flag: String) => {
      flag match {
        case "1" => "DIS_QLF"  //不合格
        case "0" => "QLF"      //合格
        case _ => null
      }
    })

    val accInsResultName = udf((flag: String) => {
      flag match {
        case "1" => "不合格"
        case "0" => "合格"
        case _ => null
      }
    })

    val accPassRate = udf((flag: String) => {
      flag match {
        case "1" => "0" // Flag 为 1 ，不合格
        case "0" => "1" // Flag 为 0 ，合格
        case _ => null
      }
    })

    val spc_detail_origin_df = CZS_Utils.getSPCOracleDF(sQLContext,spc_detail)
//              .where("ENDTIME > from_unixtime( to_unix_timestamp(now())-" + seconds + " , 'yyyy-MM-dd HH:mm:ss' )")
    val spc_detail_df = spc_detail_origin_df.withColumn("INSRESULTCODE",accInsResultCode(spc_detail_origin_df("FLAG")))
      .withColumn("INSRESULTNAME",accInsResultName(spc_detail_origin_df("FLAG")))
      .withColumn("PASSRATE",accPassRate(spc_detail_origin_df("FLAG")))

    val spc_workorder_origin_df = CZS_Utils.getSPCOracleDF(sQLContext,spc_workorder)
    val spc_workorder_df = spc_workorder_origin_df.select(spc_workorder_origin_df("ID") as "WORKORDERID",spc_workorder_origin_df("WORKORDERCODE"),
      spc_workorder_origin_df("WORKSTATUSNAME")).withColumn("WORKFLAGCODE",accWorkFlagCode(spc_workorder_origin_df("WORKSTATUSNAME")))

    val spc_bas_tag_origin_df = CZS_Utils.getSPCOracleDF(sQLContext,spc_bas_tag)
    val spc_bas_tag_df = spc_bas_tag_origin_df.select(spc_bas_tag_origin_df("ID") as "BAS_TAG_ID",spc_bas_tag_origin_df("TAGADDRESS"),spc_bas_tag_origin_df("TAGNAME"))
    val detail = spc_detail_df.join(spc_workorder_df,spc_detail_df("CORESPCMAINID") === spc_workorder_df("WORKORDERID") ,"left_outer" )
      .join(spc_bas_tag_df,spc_detail_df("TAGID") === spc_bas_tag_df("BAS_TAG_ID") ,"left_outer" )
    detail
  }


def getFinalTable(spc_detail:DataFrame,mes_wkp:DataFrame,mes_wks:DataFrame): DataFrame ={
  System.out.println("==============>>>>>    计算最终表")
  val CZS_MulAnalysisOfPhysical = spc_detail
    .join(mes_wks, spc_detail("WORKORDERCODE") === mes_wks("WORKORDERNO"), "left_outer")
    .join(mes_wkp, mes_wks("WORKCENTERCODE") === mes_wkp("WORKCENTERCODE") and (mes_wks("KQWODATE") === mes_wkp("BILLDATE"))
      and(mes_wks("PLANSTARTCLASSLABELCODE") === mes_wkp("CLASSLABELCODE") and (mes_wks("PLANSTARTCLASSGROUPCODE") === mes_wkp("CLASSGROUPCODE"))), "left_outer")
    .select(spc_detail("ID").cast(StringType), lit(3).cast(StringType), lit("卷包SPC").cast(StringType), spc_detail("WORKORDERID").cast(StringType), spc_detail("WORKORDERCODE").cast(StringType), lit(null).cast(StringType),
      lit(null).cast(StringType), lit("物理").cast(StringType), lit("AJY").cast(StringType), lit("制卷烟过程监控检验").cast(StringType), lit("抽检").cast(StringType), lit("抽检").cast(StringType),
      lit("CIGARETTE_PHYSICS").cast(StringType),lit("物理").cast(StringType) , lit("SCGD").cast(StringType), lit("生产工单").cast(StringType),
      lit("KF").cast(StringType), lit("扣分").cast(StringType), lit(null).cast(StringType), lit(null).cast(StringType), spc_detail("WORKORDERCODE"), spc_detail("WORKFLAGCODE"),
      spc_detail("WORKSTATUSNAME"), spc_detail("STARTTIME").cast(TimestampType), mes_wks("PLANSTARTCLASSLABELCODE"), mes_wks("PLANSTARTCLASSLABELNAME"), mes_wks("PLANSTARTCLASSGROUPCODE"), mes_wks("PLANSTARTCLASSGROUPNAME"),
      mes_wks("WORKCENTERCODE"), mes_wks("WORKCENTERNAME"), lit(null).cast(StringType), mes_wks("SYSTANDARDCODE"), mes_wks("MATERIALCODE"), mes_wks("MATERIALNAME"),lit(null).cast(DecimalType(8, 3)),
      lit(null).cast(DecimalType(8, 3)), lit(null).cast(DecimalType(8, 3)),  spc_detail("INSRESULTCODE"), spc_detail("INSRESULTNAME"), lit(20).cast(StringType), spc_detail("WORKORDERCODE").cast(StringType), lit(20).cast(StringType), lit("完成"),spc_detail("WORKORDERCODE").cast(StringType),
      lit(null).cast(StringType), spc_detail("WORKORDERCODE").cast(StringType), spc_detail("WORKORDERCODE").cast(StringType),lit("QTM").cast(StringType), lit("综合测试仪").cast(StringType), lit(null).cast(StringType), spc_detail("SAMPLECOUNT").cast(DecimalType(38, 0)),
      lit(null).cast(StringType), lit(null).cast(StringType), lit(100).cast(DecimalType(8, 3)), lit(null).cast(DecimalType(8, 3)), lit(null).cast(StringType), lit(null).cast(StringType), mes_wkp("STAFFNAME").cast(StringType),
      spc_detail("STARTTIME"), spc_detail("STARTTIME"), spc_detail("WORKORDERCODE").cast(StringType), lit(46).cast(StringType), lit("P_Check"), lit("JL"),
      spc_detail("TAGID").cast(StringType), spc_detail("TAGADDRESS"), spc_detail("TAGNAME"), spc_detail("CL").cast(StringType), spc_detail("LCL").cast(DecimalType(38, 0)), lit(null).cast(StringType),
      spc_detail("UCL").cast(DecimalType(38, 0)), lit(null).cast(StringType), spc_detail("CL").cast(DecimalType(38, 0)), spc_detail("SAMPLECOUNT").cast(DecimalType(38, 0)),
      spc_detail("AVG").cast(DecimalType(38, 0)), spc_detail("MAX").cast(DecimalType(19, 5)), spc_detail("MIN").cast(DecimalType(19, 5)), spc_detail("AVG").cast(DecimalType(19, 5)), spc_detail("BP").cast(DecimalType(19, 5)),
      spc_detail("CPK").cast(DecimalType(19, 5)), spc_detail("PPK").cast(DecimalType(19, 5)), lit(null).cast(DecimalType(19, 5)),lit(null).cast(StringType),
      lit(null).cast(DecimalType(38, 0)), spc_detail("QUALIFIEDSAMPLEAMOUNT").cast(DecimalType(38, 0)), lit(null).cast(DecimalType(38, 0)), lit(null).cast(DecimalType(8,3)),
      lit(null).cast(DecimalType(8, 3)), spc_detail("INSRESULTNAME"), spc_detail("PASSRATE").cast(DecimalType(19, 5)),
      lit(null).cast(DecimalType(19, 5)), mes_wks("WORKSHOPNAME"),lit("ZS").cast(StringType),lit("正式数据").cast(StringType),lit(null).cast(StringType),spc_detail("INSTRUMENTATION").cast(StringType)
    )

  CZS_MulAnalysisOfPhysical.show(5)
  println("最终大表的数据条数：" + CZS_MulAnalysisOfPhysical.count())
  CZS_MulAnalysisOfPhysical
}







}