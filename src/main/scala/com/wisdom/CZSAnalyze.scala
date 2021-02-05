package com.wisdom

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object CZSAnalyze {
    def main(args: Array[String]): Unit = {
        // 创建上下文对象
        //        val conf = new SparkConf().setMaster("local[4]").setAppName("CZSAnalyze")
        val conf = new SparkConf().setMaster("local").setAppName("CZSAnalyze")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        sc.setLogLevel("WARN")
        val arg1 = args(0)
        val arg2 = args(1)
//        val arg1 = 1
//        val arg2 ="hour"
        var seconds = 0
        if (arg2 == "hour") {
            seconds = arg1.toInt * 3600
        } else if (arg2 == "day") {
            seconds = arg1.toInt * 24 * 3600
        } else {
            throw new Exception("参数格式错误：'"+arg2+"'")
        }
        
        // 读取明细表
        System.out.println("==============>>>>>    读取明细表")
        val orignDetail = sqlContext.read.format("org.apache.phoenix.spark")
            .option("table", "PD_KQMES_QTY.QTY_INSWORKORDERTASKDETAIL2")
            .option("zkUrl", "master.cm.com,master2.cm.com,node1.cm.com:2181")
            .load()
            // 在这里指定计算的时间区间，以及排除无效的记录
//            .where("OPRATIONDATE>from_unixtime( to_unix_timestamp(now())-" + seconds + " , 'yyyy-MM-dd HH:mm:ss' ) AND ITEMMEAN IS NOT NULL AND ITEMMEAN !=0.0")
        val detail = orignDetail.withColumn("FullRate", (orignDetail("TOTALDEDUCTION") === 0.0).cast("double"))
            .withColumn("PassRate", (orignDetail("DEFECTSAMPLENUMBER") === 0.0).cast("double"))


      detail.show()
        // 读取样本表
        System.out.println("==============>>>>>    读取样本表")
        val sample = sqlContext.read.format("org.apache.phoenix.spark")
            .option("table", "PD_KQMES_QTY.QTY_INSWORKORDERTASKDETAILVAL")
            .option("zkUrl", "master.cm.com,master2.cm.com,node1.cm.com:2181")
            .load()
            .select("INSWORKORDERTASKDETAILID", "SAMPLEVALUE")
        sample.show()
        // 选取更小的上下限差值
        System.out.println("==============>>>>>    计算差值")
        val originDiff = detail.select(detail("ID"), detail("ITEMMEAN"), detail("SAMPLENUMBER"), detail("DISPLAYUPPERLIMIT") - detail("ITEMMEAN") as "upper", detail("ITEMMEAN") - detail("DISPLAYLOWERLIMIT") as "lower")
        val diff = originDiff.withColumn("DIFF", originDiff("upper") * (originDiff("upper") <= originDiff("lower")).cast("int") + originDiff("lower") * (originDiff("upper") > originDiff("lower")).cast("int"))
            .select("ID", "ITEMMEAN", "SAMPLENUMBER", "DIFF")

        originDiff.show()
        // 计算 Sigma
        System.out.println("==============>>>>>    计算sigma")
        val sigma = detail.join(sample, detail("ID") === sample("INSWORKORDERTASKDETAILID"), "left_outer")
            .groupBy("ID")
            .agg(sqrt(sum((sample("SAMPLEVALUE") - detail("ITEMMEAN")) * (sample("SAMPLEVALUE") - detail("ITEMMEAN"))) / (first(detail("SAMPLENUMBER")) - 1)) as "SIGMA")
//          .agg(stddev("") as "sigma")


      sigma.show()
        // 计算PPK
        System.out.println("==============>>>>>    计算ppk")
        val ppk = diff.select("ID", "DIFF").join(sigma, Seq("ID"), "left_outer")
            .withColumn("itemPpk", diff("DIFF") / (sigma("SIGMA") * 3))
            .select("ID", "itemPpk")
        ppk.show()

        // 计算子组信息
        System.out.println("==============>>>>>    计算子组明细")
        val detail2 = detail.join(ppk, detail("ID") === ppk("ID"), "inner")
            .select(detail("ID"), detail("INSWORKORDERTASKID"), detail("PROCESS03"), detail("ITEMTYPE"), detail("INSITEMQUACHARACTER"), detail("ITEMID"),
                detail("ITEMCODE"), detail("ITEMNAME"), detail("VALUESTD"), detail("VALUESTDLOWERLIMIT"), detail("LEFTTYPE"), detail("VALUESTDUPPERLIMIT"),
                detail("RIGHTTYPE"), detail("VALUESTDCENTER"), detail("SAMPLENUMBER"), detail("INSVALUE"), detail("ITEMMAX"), detail("ITEMMIN"), detail("ITEMMEAN"),
                detail("ITEMSTDDEV"), detail("ITEMCPK"), ppk("itemPpk"), detail("ITEMVAR"), detail("DEFECTTYPE"), detail("DEFECTSAMPLENUMBER"),
                detail("SAMPLENUMBER") - detail("DEFECTSAMPLENUMBER") as "QUALIFYSAMPLENUMBER", detail("DEFECTDEDUCTION"), detail("TOTALDEDUCTION"),
                (lit(100) - detail("TOTALDEDUCTION")) as "SCORE", detail("INSRESULT"), detail("PassRate"), detail("FullRate"))
        detail2.show()

        println("读取工单信息表")
        val origin_order = sqlContext.read.format("org.apache.phoenix.spark")
            .option("table", "PD_KQMES_QTY.QTY_INSWORKORDER")
            .option("zkUrl", "master.cm.com,master2.cm.com,node1.cm.com:2181")
            .load()

      val bas_finishedmat_df = sqlContext.read.format("org.apache.phoenix.spark")
        .option("table", "PD_KQMES.BAS_FINISHEDMAT")
        .option("zkUrl", "master.cm.com,master2.cm.com,node1.cm.com:2181")
        .load()
      val bas_finishedmat = bas_finishedmat_df.select("MATCODE","SYSTANDARDCODE")
      val order = origin_order.join(bas_finishedmat,origin_order("MATERIELCODE")=== bas_finishedmat("MATCODE"),"left")

      println("读取任务信息表")
        val task = sqlContext.read.format("org.apache.phoenix.spark")
            .option("table", "PD_KQMES_QTY.QTY_INSWORKORDERTASK2")
            .option("zkUrl", "master.cm.com,master2.cm.com,node1.cm.com:2181")
            .load()
            // 追加两个不存在的字段
            .withColumn("TOTALSCORE", lit(null).cast(DoubleType))
            .withColumn("SCORE", lit(null).cast(DoubleType))

//         读取加工中心信息表
        val workCenter = sqlContext.read.format("org.apache.phoenix.spark")
            .option("table", "PD_KQMES.BAS_WORKCENTER")
            .option("zkUrl", "master.cm.com,master2.cm.com,node1.cm.com:2181")
            .load()

        // 读取加工中心设备信息表
        val equipment = sqlContext.read.format("org.apache.phoenix.spark")
            .option("table", "PD_KQMES.BAS_WORKCENTER_EQP_REL")
            .option("zkUrl", "master.cm.com,master2.cm.com,node1.cm.com:2181")
            .load()


        // 关联处理
        System.out.println("==============>>>>>    计算最终表")
        val CZS_MulAnalysisOfPhysical = detail2.join(task, task("ID") === detail2("INSWORKORDERTASKID"), "left_outer")
            .join(order, order("ID") === task("INSWORKORDERID"), "left_outer")
            .join(equipment, equipment("WORKCENTERCODE") === order("WORKCENTERCODE"), "left_outer")
            .join(workCenter, workCenter("WORKCENTERCODE") === order("WORKCENTERCODE"), "left_outer")
            .select(detail2("ID"), lit("1"), lit("过程检测"), order("ID"), order("INSORDERNO"), order("STDCODE"), order("STDNAME"), order("QUACLASS"), order("INSSTAGECODE"),
                order("INSSTAGENAME"), order("INSFORMCODE"), order("INSFORMNAME"), order("INSTYPECODE"), order("INSTYPENAME"), order("INSBASISCODE"), order("INSBASISNAME"),
                order("JUDGEMETHODCODE"), order("JUDGEMETHODNAME"), order("STDVERSIONID"), order("STDVERSION"), order("WORKORDERNO"), order("WORKFLAGCODE"),
                order("WORKFLAGNAME"), order("WORKDATE"), order("WORKCLASSLABELCODE"), order("WORKCLASSLABELNAME"), order("WORKCLASSGROUPCODE"), order("WORKCLASSGROUPNAME"),
                order("WORKCENTERCODE"), order("WORKCENTERNAME"), equipment("EQUIPMENTID"), order("SYSTANDARDCODE"), order("MATERIELCODE"), order("MATERIELNAME"), order("SCORE"),
                order("PASSMARK"), order("TOTALSCORE"), order("INSRESULTCODE"), order("INSRESULTNAME"), order("BILLSTATUS"), task("ID"), task("STATUSCODE"), task("STATUSNAME"), task("INSWORKORDERID"),
                task("INSSTDVERSIONTASKID"), task("INSTASKCODE"), task("INSTASKNAME"), task("INSEQUCLASSCODE"), task("INSEQUCLASSNAME"), task("INSINSTRUMENTID"), task("SAMPLENUMBER"),
                task("TOTALDEDUCTION"), task("DEDUCTREASON"), task("TOTALSCORE"), task("SCORE"), task("CADDYMARK"), task("CARTONMARK"), task("INSUSER"), task("SAMPLETIME"), task("INSTIME"),
                detail2("INSWORKORDERTASKID"), detail2("PROCESS03"), detail2("ITEMTYPE"), detail2("INSITEMQUACHARACTER"), detail2("ITEMID"), detail2("ITEMCODE"), detail2("ITEMNAME"),
                detail2("VALUESTD"), detail2("VALUESTDLOWERLIMIT"), detail2("LEFTTYPE"), detail2("VALUESTDUPPERLIMIT"), detail2("RIGHTTYPE"), detail2("VALUESTDCENTER"), detail2("SAMPLENUMBER"),
                detail2("INSVALUE"), detail2("ITEMMAX"), detail2("ITEMMIN"), detail2("ITEMMEAN"), detail2("ITEMSTDDEV"), detail2("ITEMCPK"), detail2("itemPpk"), detail2("ITEMVAR"), detail2("DEFECTTYPE"),
                detail2("DEFECTSAMPLENUMBER"), detail2("QUALIFYSAMPLENUMBER"), detail2("DEFECTDEDUCTION"), detail2("TOTALDEDUCTION"), detail2("SCORE"), detail2("INSRESULT"), detail2("PassRate"),
                detail2("FullRate"), workCenter("WORKSHOPNAME"),order("DATAGOALCODE"),order("DATAGOALNAME"),task("INSTRUMENTCODE"),task("INSTRUMENTNAME"))

      CZS_MulAnalysisOfPhysical.show()
      println("最终大表的数据条数：" + CZS_MulAnalysisOfPhysical.count())
        // 处理结果写出到Hive
        val hdfsConf = sc.hadoopConfiguration
        val hdfs = FileSystem.get(new URI("/"), hdfsConf, "hdfs")
        val path = new Path("/user/hive/warehouse/pd_czs.db/czs_mulanalysisofphysical")
        if (hdfs.exists(path)) {
            hdfs.delete(path, true)
        }
        // 删除DF输出时的括号
        // 本质就是直接将每条记录转化为一个字符串然后删除两侧的括号
        System.out.println("==============>>>>>    写出结果")
        CZS_MulAnalysisOfPhysical.map(row => {
            val str = row.toString
            str.substring(1, str.length() - 1).replaceAll("null", "")
        }).repartition(10).saveAsTextFile("/user/hive/warehouse/pd_czs.db/czs_mulanalysisofphysical")

    }
}
