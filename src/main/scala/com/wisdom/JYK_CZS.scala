package com.wisdom

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

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
  * @Date 2020/11/11 10:59
  */
object JYK_CZS {
  private val logger = LoggerFactory.getLogger(JYK_CZS.getClass)

  val conf = new SparkConf().setAppName("JYK_CZS")
//    .setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  sc.setLogLevel("WARN")

  val system="JYK"
  val zkUrl = "master.cm.com,master2.cm.com,node1.cm.com:2181"
  //Phoenix JYK TABLE
  val TABLE_JYK_CLZ = "PD_JYK.O_JYK_WSD_QTMCD_CLZ" //物测QTM长度测量值
  val TABLE_JYK_PACKROLL_DETAIL = "PD_JYK.O_JYK_WSD_PACKROLL_DETAIL" //包装卷纸详细信息表
  val TABLE_JYK_QX_SCORE = "PD_JYK.O_JYK_WSD_QX_SCORE" //缺陷信息表
  val TABLE_JYK_SAMPLE_SCORE = "PD_JYK.O_JYK_WSD_SAMPLE_SCORE" //样品得分表

  //Oracle CZS TABLE
  val TABLE_CZS_QTY_INSWORKORDERTASKDETAILVAL = "PDCZS.QTY_INSWORKORDERTASKDETAILVAL"
  val TABLE_CZS_DETAIL_VALUE = "PDCZS.CZS_MULANALYSISOFPHYSICAL" // 多为物联分析
  val TABLE_CZS_TDM_NEWCLASS = "PDCZS.TDM_NEWCLASS" //班次班别基础表
  val TABLE_CZS_TDM_NEWBOARD = "PDCZS.TDM_NEWBOARD" //过程检测与三级站机台对应关系（与物测多维通用表）
  val TABLE_CZS_TDM_SJZITEMNAME = "PDCZS.TDM_SJZITEMNAME" //检测项目维度表
  val TABLE_CZS_BAS_FINISHEDMAT = "PDCZS.BAS_FINISHEDMAT" //成品代码
  val TABLE_CZS_QTMCD_CLZ= "PDCZS.O_JY_WSD_QTMCD_CLZ"

  val prop = {
    val p = new java.util.Properties()
    p.put("driver", "oracle.jdbc.OracleDriver")
    p.put("user", "PDCZS")
    p.put("password", "password123456")
    p
  }


  def main(args: Array[String]): Unit = {
    val arg1 = args(0)
    val arg2 = args(1)
    var seconds = 0
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

    createPhoenixRegisterTable(sqlContext, TABLE_JYK_CLZ)
    createPhoenixRegisterTable(sqlContext, TABLE_JYK_PACKROLL_DETAIL)

    val jyk_origin_detail = getPhoenixReader(sqlContext, TABLE_JYK_PACKROLL_DETAIL).load()
    val jyk_sample_number = getSampleNumber_Register(sqlContext)
    // ===============>>>>   获取子组信息表包含 SampleNumber
    val jyk_detail_with_sampleNumber = getZiZuDetail(sqlContext, jyk_origin_detail, jyk_sample_number, seconds)
    // ===============>>>>   获取 Diff ,Sigma
    val jyk_sample_value = getSampleValue_Register(sqlContext)
    val diff = accumulateMinDiff(sqlContext, jyk_origin_detail)
    val sigma = accumulateSigma(sqlContext, jyk_detail_with_sampleNumber, jyk_sample_value)
    // ===============>>>>   求 PPK 与 CPK
    val ppk_and_cpk = accumulatePPKAndCPK(sqlContext, jyk_detail_with_sampleNumber, jyk_sample_value, diff, sigma)
    //===============>>>>    获取 SampleScore 和 判定结果Code
    val jyk_sample_score = getPhoenixReader(sqlContext, TABLE_JYK_SAMPLE_SCORE).load()
    val sample_score_inscode = getSampleScore(sqlContext, jyk_sample_score)
    // ===============>>>>   获取QX_SCORE 数据包含 满分率和合格率
    val qx_score_final = getQXScoreFullPass(sqlContext)
    //===============>>>>    获取 TDM_NewClass,Bas_FinishedMat,JYK_Sample_Score
    val sample_score_final = getClassFinishedMatSampleScore(sqlContext, sample_score_inscode)
    //===============>>>>    获取 TDM_NewBoard,TDM_SIJZZFENAME,JYK_Detail
    val detail = getTdmSjzzfeNameDetail(sqlContext, jyk_detail_with_sampleNumber)
    //===============>>>>    获取 TDM_NewBoard,TDM_SIJZZFENAME,JYK_Detail,PPK CPK
    val detail_final = getFinalDetail(sqlContext, detail, ppk_and_cpk)
//    detail_final.show(10)
    // ==============>>>     获取最终的大表
    logger.warn("开始获取最终表")
    val czs_final = getFinalTable(sqlContext, detail_final, sample_score_final, qx_score_final)
    logger.warn(s"开始将数据写入 Oracle,大表czs_final的数据量为:${czs_final.count()}")
    logger.warn(s"开始将${TABLE_CZS_DETAIL_VALUE}数据导入HDFS ")
    DfToHdfs(czs_final,TABLE_CZS_DETAIL_VALUE)

//    czs_final.filter("ID=10023119").show(100)
//    writeOracleTable(czs_final, TABLE_CZS_DETAIL_VALUE, SaveMode.Append)


    //===============>>>     获取 QTY_VAL 表并传入到Oracle中
    val clz_qty_val: DataFrame = getClzCzsQtyVal(sqlContext,seconds)
    logger.warn(s"获取 QTY_VAL 表并传入到Oracle中,明细表clz_qty_val的数据量为:${clz_qty_val.count()}")
    logger.warn(s"开始将${TABLE_CZS_QTY_INSWORKORDERTASKDETAILVAL}数据导入HDFS ")
    DfToHdfs(clz_qty_val,TABLE_CZS_QTY_INSWORKORDERTASKDETAILVAL)

    //    writeOracleTable(clz_qty_val, TABLE_CZS_QTY_INSWORKORDERTASKDETAILVAL, SaveMode.Append)

    // ==============>>>    获取 QTMCD_CLZ 表并传入到 Oracle 中
    val czs_clz: DataFrame = getQtmcdClz(sqlContext,seconds)
    logger.warn(s"获取 QTMCD_CLZ 表并传入到Oracle中,明细表 QTMCD_CLZ 的数据量为:${czs_clz.count()}")
    logger.warn(s"开始将${TABLE_CZS_QTMCD_CLZ}数据导入HDFS ")
    DfToHdfs(czs_clz,TABLE_CZS_QTMCD_CLZ)
//    writeOracleTableWithoutDrop(czs_clz, TABLE_CZS_QTMCD_CLZ, SaveMode.Append)

  }

  def getPhoenixReader(sQLContext: SQLContext, tableName: String): DataFrameReader = {
    // 2.使用 phoenix-spark 直接连接Phoenix集群 (org.apache.phoenix.spark)
    sQLContext.read
      .format("org.apache.phoenix.spark")
      .options(Map("table" -> tableName, "zkUrl" -> zkUrl))
  }

  def createPhoenixRegisterTable(sQLContext: SQLContext, tableName: String): Unit = {
    val rdf = sQLContext.load("org.apache.phoenix.spark", Map("table" -> tableName, "zkUrl" -> zkUrl))
    println(tableName.substring(7))
    rdf.registerTempTable(tableName.substring(7))
  }


  def getClzCzsQtyVal(sQLContext: SQLContext,seconds:Int): DataFrame = {
    val getDate = udf(() => {
      val now: Date = new Date()
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date = dateFormat.format(now)
      date
    })

    val czs_detail_df = sqlContext.sql(

      s"""SELECT CONCAT(a.DETAIL_ID,b.TESTNO) as ID,CAST(a.DETAIL_ID AS STRING),CAST(b.TESTNO AS DECIMAL(38,10)),
                      CASE WHEN a.ITEM_NAME='圆周' THEN b.CIRCLE
                      WHEN a.ITEM_NAME='长度' THEN b.LENGTH
                      WHEN a.ITEM_NAME='硬度' THEN b.RIGID
                      WHEN a.ITEM_NAME='质量' THEN b.WEIGHT
                      WHEN a.ITEM_NAME='吸阻' THEN b.RESIST
                      WHEN a.ITEM_NAME='总通风率' THEN b.VENT
                      ELSE NULL END SAMPLEVALUE
                      FROM  O_JYK_WSD_PACKROLL_DETAIL a join O_JYK_WSD_QTMCD_CLZ b on a.SAMPLE_ID=b.SAMPLE_ID """)

    val czs_qty = czs_detail_df.select(czs_detail_df("ID"), czs_detail_df("DETAIL_ID"), czs_detail_df("TESTNO"), czs_detail_df("SAMPLEVALUE").cast("DECIMAL(18,5)"), lit(null), lit(null), lit(null), lit(null), lit(null), lit(null), lit(null), lit(null), lit(null), lit(null), lit(null), lit(0).cast("DECIMAL(38,10)"), lit("空"), getDate().cast("Timestamp"), lit(null), lit(null), lit(null), lit("空"), lit(null))

//    println("CZS VALUE :" + czs_detail_df.schema)
    val schema = getOracleDF(sQLContext, TABLE_CZS_QTY_INSWORKORDERTASKDETAILVAL).schema

    val czs_qty_val_final = sQLContext.createDataFrame(czs_qty.rdd, schema)
    czs_qty_val_final.show(10)
    czs_qty_val_final
  }


  def getQtmcdClz(sQLContext: SQLContext,seconds:Int):DataFrame= {
    val origin_jyk_clz = getPhoenixReader(sQLContext,TABLE_JYK_CLZ).load()
    val jyk_clz = origin_jyk_clz.select(origin_jyk_clz("SAMPLE_ID").cast(DecimalType(38,10)),origin_jyk_clz("TESTNO").cast(DecimalType(38,10)),origin_jyk_clz("LENGTH"),origin_jyk_clz("CIRCLE"),origin_jyk_clz("RIGID"),origin_jyk_clz("WEIGHT"),origin_jyk_clz("RESIST"),origin_jyk_clz("VENT"),origin_jyk_clz("SENDTIME"),origin_jyk_clz("OP_MODE"))
//      .where("SENDTIME > from_unixtime( to_unix_timestamp(now())-" + seconds + " , 'yyyy-MM-dd HH:mm:ss' )")
//    logger.warn(s"origin jyk clz Schema : ${jyk_clz.schema}")
    val oracle_schema = getOracleDF(sQLContext,TABLE_CZS_QTMCD_CLZ).schema
//    logger.warn(s"origin czs clz Schema : ${oracle_schema}")
    val czs_clz = sQLContext.createDataFrame(jyk_clz.rdd,oracle_schema)
//    logger.warn(s"final clz Schema: ${czs_clz.schema}")
    czs_clz
  }

  def getSampleValue_Register(sQLContext: SQLContext): DataFrame = {
    val sample_value = sqlContext.sql(
      s"""SELECT a.DETAIL_ID,b.SAMPLE_ID,a.ITEM_NAME,
                      CASE WHEN a.ITEM_NAME='圆周' THEN b.CIRCLE
                      WHEN a.ITEM_NAME='长度' THEN b.LENGTH
                      WHEN a.ITEM_NAME='硬度' THEN b.RIGID
                      WHEN a.ITEM_NAME='质量' THEN b.WEIGHT
                      WHEN a.ITEM_NAME='吸阻' THEN b.RESIST
                      WHEN a.ITEM_NAME='总通风率' THEN b.VENT
                      ELSE NULL END SAMPLEVALUE
                      FROM  O_JYK_WSD_PACKROLL_DETAIL a join O_JYK_WSD_QTMCD_CLZ b on a.SAMPLE_ID=b.SAMPLE_ID""")
    sample_value
  }

  def getSampleNumber_Register(sQLContext: SQLContext): DataFrame = {
    val sample_number = sQLContext.sql("SELECT SAMPLE_ID as SAMPLE_SAMPLE_ID,count(1) as SAMPLENUMBER FROM  O_JYK_WSD_QTMCD_CLZ group by SAMPLE_ID")
    sample_number
  }


  def getZiZuDetail(sQLContext: SQLContext, detailDF: DataFrame, sampleNumber: DataFrame, seconds: Int): DataFrame = {
    val jyk_new_detail = detailDF.join(sampleNumber, detailDF("SAMPLE_ID") === sampleNumber("SAMPLE_SAMPLE_ID"), "left_outer")
//          .where("SENDTIME > from_unixtime( to_unix_timestamp(now())-" + seconds + " , 'yyyy-MM-dd HH:mm:ss' )")
    jyk_new_detail
  }

  def getQXScoreFullPass(sQLContext: SQLContext): DataFrame = {
    val jyk_qx_score = getPhoenixReader(sqlContext, TABLE_JYK_QX_SCORE).load()
    val qx_score_full_pass = jyk_qx_score
      .where(" ITEMGROUP_NAME='卷制' ")
      .withColumn("FullRate", (jyk_qx_score("QX_SCORE") === 0.0).cast("double"))
      .withColumn("PassRate", (jyk_qx_score("QX_NUM") === 0.0).cast("double"))
      .withColumn("SCORE", lit(100) - jyk_qx_score("QX_SCORE"))
    qx_score_full_pass
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

  def getTdmNewBoard(sQLContext: SQLContext, zizu_detail: DataFrame): DataFrame = {
    val czs_new_board = getOracleDF(sQLContext, TABLE_CZS_TDM_NEWBOARD)
    val detail_sample_czs_board = zizu_detail.join(czs_new_board, zizu_detail("JTH_NAME_YC") === czs_new_board("MACHINECODE"), "left_outer")
    detail_sample_czs_board
  }

  def getTdmSjzzfeNameDetail(sQLContext: SQLContext, zizu_detail: DataFrame): DataFrame = {
    val detail_sample_board = getTdmNewBoard(sQLContext, zizu_detail)
    val czs_new_sijitename = getOracleDF(sQLContext, TABLE_CZS_TDM_SJZITEMNAME)
    val final_jyk_detail = detail_sample_board.join(czs_new_sijitename, detail_sample_board("ITEM_NAME") === czs_new_sijitename("SJZITEMNAME"), "left_outer")
    //    println(s"getTdmSjzzfeNameDetail Count: ${final_jyk_detail.count()}")
    println(s"${final_jyk_detail.schema}")
    final_jyk_detail
  }

  def getFinalDetail(sQLContext: SQLContext, zizu_detail_without_ppk: DataFrame, ppk_cpk: DataFrame): DataFrame = {
    val final_detail_with_CpK = zizu_detail_without_ppk.join(ppk_cpk, zizu_detail_without_ppk("DETAIL_ID") === ppk_cpk("CpkPpk_DETAIL_ID"), "left_outer")
    //    println(s"final_detail_with_CpK Count: ${final_detail_with_CpK.count()}")
    final_detail_with_CpK
  }

  def getTdmNewClassScore(sQLContext: SQLContext, sample_score: DataFrame): DataFrame = {
    val trimKong = udf((name: String) => {
     val str = name.trim()
      str
    })
    val czs_new_class = getOracleDF(sQLContext, TABLE_CZS_TDM_NEWCLASS)
    val new_czs_class = sample_score.join(czs_new_class, trimKong(sample_score("BANCI_NAME")) === czs_new_class("WORKCLASSLABELNAME"), "left_outer")
//    new_czs_class.show(10)
    new_czs_class
  }

  def getClassFinishedMatSampleScore(sQLContext: SQLContext, sample_score: DataFrame): DataFrame = {
    val class_sample_score = getTdmNewClassScore(sQLContext, sample_score)
    val czs_bas_finishedmat = getOracleDF(sQLContext, TABLE_CZS_BAS_FINISHEDMAT)
    val class_finishmat_sample_score = class_sample_score.join(czs_bas_finishedmat, class_sample_score("PRODUCT_ID") === czs_bas_finishedmat("SYSTANDARDCODE"), "left_outer")
    //    println(s"class_finishmat_sample_score Count：${class_finishmat_sample_score.count()}")
    class_finishmat_sample_score
  }


  def accumulateSigma(sQLContext: SQLContext, detailDF: DataFrame, sampleDF: DataFrame): DataFrame = {
    val sigma = detailDF.join(sampleDF, detailDF("DETAIL_ID") === sampleDF("DETAIL_ID"), "left_outer")
      .groupBy(detailDF("DETAIL_ID"))
      .agg(sqrt(sum((sampleDF("SAMPLEVALUE") - detailDF("AVGVAL")) * (sampleDF("SAMPLEVALUE") - detailDF("AVGVAL"))) / (first(detailDF("SAMPLENUMBER")) - 1)) as "SIGMA")
    //      .agg(avg("cmp_sex"), stddev("cmp_sex"))
    sigma
  }

  def accumulateMinDiff(sQLContext: SQLContext, jyk_origin_detail: DataFrame): DataFrame = {
    val originDiff = jyk_origin_detail.select(jyk_origin_detail("SAMPLE_ID"), jyk_origin_detail("DETAIL_ID"), jyk_origin_detail("ITEM_NAME"), jyk_origin_detail("UP_LIMIT") - jyk_origin_detail("AVGVAL") as "upper", jyk_origin_detail("AVGVAL") - jyk_origin_detail("DOWN_LIMIT") as "lower")
    val diff = originDiff.withColumn("DIFF", originDiff("upper") * (originDiff("upper") <= originDiff("lower")).cast("int") + originDiff("lower") * (originDiff("upper") > originDiff("lower")).cast("int"))
      .select("DETAIL_ID", "SAMPLE_ID", "ITEM_NAME", "DIFF")
    diff
  }

  def accumulatePPKAndCPK(sQLContext: SQLContext, detailDF: DataFrame, sampleDF: DataFrame, diff: DataFrame, sigma: DataFrame): DataFrame = {
    val ppk_and_cpk = diff.select("DETAIL_ID", "DIFF").join(sigma, Seq("DETAIL_ID"), "left_outer")
      .withColumn("ITEMPPK", diff("DIFF") / (sigma("SIGMA") * 3))
      .withColumn("ITEMCPK", diff("DIFF") / ((sigma("SIGMA") / 0.9896) * 3))
    val cpk_ppk_detaild_id = ppk_and_cpk.select(ppk_and_cpk("DETAIL_ID") as "CpkPpk_DETAIL_ID", ppk_and_cpk("ITEMPPK"), ppk_and_cpk("ITEMCPK"))
    cpk_ppk_detaild_id
  }

  def getFinalTable(sQLContext: SQLContext, final_detail: DataFrame, final_sample_score: DataFrame, final_qx_score: DataFrame): DataFrame = {
    println(s"Final Detail Schema：${final_detail.schema}")
    val CZS_MulAnalysisOfPhysical: DataFrame = final_detail
      .join(final_qx_score, final_detail("ITEM_NAME") === final_qx_score("ITEM_NAME") and final_detail("SAMPLE_ID") === final_qx_score("SAMPLE_ID"), "left_outer")
      .join(final_sample_score, final_detail("SAMPLE_ID") === final_sample_score("SAMPLE_ID"), "left_outer")
      .select(final_detail("DETAIL_ID").cast(StringType), lit(2).cast(StringType), lit("三级站").cast(StringType), final_sample_score("SAMPLE_ID").cast(StringType), final_sample_score("SAMPLE_ID").cast(StringType), lit(null).cast(StringType),
        lit(null).cast(StringType), lit("P").cast(StringType), lit(null).cast(StringType), lit(null).cast(StringType), lit(null).cast(StringType), final_sample_score("TEST_CATEGORY"), lit(null).cast(StringType), final_qx_score("ITEMGROUP_NAME"), lit(null).cast(StringType), lit(null).cast(StringType),
        lit(null).cast(StringType), lit(null).cast(StringType), lit(null).cast(StringType), lit(null).cast(StringType), lit(null).cast(StringType), lit(null).cast(StringType),
        lit(null).cast(StringType), final_sample_score("PRODUCT_DATE").cast(TimestampType), final_sample_score("WORKCLASSLABELCODE"), final_sample_score("BANCI_NAME"), final_sample_score("WORKCLASSGROUPCODE"), final_sample_score("BANBIE_NAME"),
        final_detail("WORKCENTERCODE"), final_detail("WORKCENTERNAME"), lit(null).cast(StringType), final_sample_score("PRODUCT_ID"), final_sample_score("MATCODE"), final_sample_score("MATNAME"), final_sample_score("PACK_ROLL_SCORE").cast(DecimalType(8, 3)),
        lit(null).cast(DecimalType(8, 3)), lit(100).cast(DecimalType(8, 3)), final_sample_score("INSRESULTCODE"), final_sample_score("PACK_ROLL_JUDGE_NAME"), lit(20).cast(StringType), final_qx_score("SAMPLE_ID").cast(StringType), lit(20).cast(StringType), lit("完成"), final_sample_score("SAMPLE_ID").cast(StringType),
        lit(null).cast(StringType), final_sample_score("SAMPLE_ID").cast(StringType), final_sample_score("SAMPLE_ID").cast(StringType), final_detail("INSEQUCLASSCODE"), final_detail("INSEQUCLASSNAME"), lit(null).cast(StringType), final_detail("SAMPLENUMBER").cast(DecimalType(38, 0)),
        (lit(100) - final_sample_score("PACK_ROLL_SCORE")).cast(StringType), final_qx_score("QX_NAME"), lit(100).cast(DecimalType(8, 3)), final_sample_score("PACK_ROLL_SCORE").cast(DecimalType(8, 3)), lit(null).cast(StringType), lit(null).cast(StringType), lit(null).cast(StringType), final_sample_score("PRODUCT_DATE"), final_sample_score("TEST_DATE"),
        final_sample_score("SAMPLE_ID").cast(StringType), lit(46).cast(StringType), lit("P_Check"), lit("JL"), lit(null).cast(StringType), final_detail("ITEMCODE"), final_detail("ITEMNAME"),
        final_detail("DESIGN"), final_detail("DOWN_LIMIT").cast(DecimalType(8, 3)), lit(null).cast(StringType), final_detail("UP_LIMIT").cast(DecimalType(8, 3)), lit(null).cast(StringType), ((final_detail("DOWN_LIMIT") + final_detail("UP_LIMIT")) / 2).cast(DecimalType(38, 0)), final_detail("SAMPLENUMBER").cast(DecimalType(38, 0)),
        final_detail("DESIGN").cast(DecimalType(38, 0)), final_detail("MAXVAL").cast(DecimalType(19, 5)), final_detail("MINVAL").cast(DecimalType(19, 5)), final_detail("AVGVAL").cast(DecimalType(19, 5)), final_detail("STDVAL").cast(DecimalType(19, 5)), final_detail("ITEMCPK").cast(DecimalType(19, 5)), final_detail("ITEMPPK").cast(DecimalType(19, 5)), final_detail("VARVAL").cast(DecimalType(19, 5)), final_qx_score("QXTYPE_NAME"),
        final_qx_score("QX_NUM").cast(DecimalType(38, 0)), (final_detail("SAMPLENUMBER") - final_qx_score("QX_NUM")).cast(DecimalType(38, 0)), lit(null).cast(DecimalType(38, 0)), final_qx_score("QX_SCORE").cast(DoubleType), final_qx_score("SCORE").cast(DecimalType(8, 3)), final_detail("JUDGE_NAME"), final_qx_score("PassRate").cast(DecimalType(19, 5)),
        final_qx_score("FullRate").cast(DecimalType(19, 5)), final_sample_score("WORKSHOP_NAME"),lit(null).cast(StringType),lit(null).cast(StringType),lit(null).cast(StringType),lit(null).cast(StringType)
      )
//        CZS_MulAnalysisOfPhysical.show(10)
    logger.warn(s"final_join_CZS_MulAnalysisOfPhysical 的schema ：${CZS_MulAnalysisOfPhysical.schema}")
    //    CZS_MulAnalysisOfPhysical.show()
    //    logger.warn(s"CZS_MulAnalysisOfPhysical 的Count： ${CZS_MulAnalysisOfPhysical.count()}")
    //    CZS_MulAnalysisOfPhysical.show(10)
//    val origin_schema = getOracleDF(sQLContext, TABLE_CZS_DETAIL_VALUE).schema
//    logger.warn(s"origin_oracle_CZS_MulAnalysisOfPhysical schema:$origin_schema")
//    val CZS_MulAnalysisOfPhysical_final = sQLContext.createDataFrame(CZS_MulAnalysisOfPhysical.rdd, origin_schema)
//        .filter("SCORE IS NOT NULL")
    //    CZS_MulAnalysisOfPhysical
    CZS_MulAnalysisOfPhysical.show(10)
//    logger.warn(s"最终大表的数据为  ${CZS_MulAnalysisOfPhysical_final.count()}")
    CZS_MulAnalysisOfPhysical
  }


  def getOracleDF(sQLContext: SQLContext, tableName: String): DataFrame = {
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

  def DfToHdfs(dataFrame:DataFrame,tableName:String): Unit ={
    // 处理结果写出到Hive
    val hdfsConf = sc.hadoopConfiguration
    val hdfs = FileSystem.get(new URI("/"), hdfsConf, "hdfs")
    val path = new Path(s"/user/hive/warehouse/pd_czs.db/${tableName.split("\\.")(1).toLowerCase()}_${system.toLowerCase}")
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)
    }
    // 删除DF输出时的括号
    // 本质就是直接将每条记录转化为一个字符串然后删除两侧的括号
    System.out.println("==============>>>>>    写出结果")
    dataFrame.map(row => {
      val str = row.toString
      str.substring(1, str.length() - 1).replaceAll("null", "")
    }).repartition(10).saveAsTextFile(s"/user/hive/warehouse/pd_czs.db/${tableName.split("\\.")(1).toLowerCase()}_${system.toLowerCase}")

    logger.warn(s"${tableName} 写入HDFS 成功！")

  }

  def writeOracleTable(dataFrame: DataFrame, tableName: String, saveMode: SaveMode): Unit = {
    dataFrame
      .dropDuplicates(Seq("ID"))
      .repartition(1)
      .write.mode(SaveMode.Append).jdbc("jdbc:oracle:thin:@//10.10.184.60/PDCZS", tableName, prop)
    logger.warn(s"落地oracle表${tableName}成功")
  }

  def writeOracleTableWithoutDrop(dataFrame: DataFrame, tableName: String, saveMode: SaveMode): Unit = {
    dataFrame
      .repartition(1)
      .write.mode(SaveMode.Append).jdbc("jdbc:oracle:thin:@//10.10.184.60/PDCZS", tableName, prop)
    logger.warn(s"落地oracle表${tableName}成功")
  }
}
