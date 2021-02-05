package com.utils

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object ExportTable {
    def main(args: Array[String]): Unit = {
        // 读取参数
        // 要导出的表格
        val table = args(0)
        // 记录导出表格的文件路径
        val columns = args(1)
        // 用于判断时间的字段
        val dateColumn = args(2)
        // 时间数量
        val num = args(3)
        // 时间单位
        val unit = args(4)
        // 数据导出的目标地址
        val targetPath=args(5)
        
        // 判断要减去的秒数
        var seconds = 0
        if (unit == "hour") {
            seconds = num.toInt * 3600
        } else if (unit == "day") {
            seconds = num.toInt * 24 * 3600
        } else {
            throw new Exception("参数格式错误")
        }
        
        // 创建上下文对象
        //        val conf = new SparkConf().setMaster("local[4]").setAppName("CZSAnalyze")
        val conf = new SparkConf().setMaster("yarn-cluster").setAppName("ExportTable")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        
        // 读取表格
        val originData = sqlContext.read.format("org.apache.phoenix.spark")
            .option("table", table)
            .option("zkUrl", "master.cm.com,master2.cm.com,node1.cm.com:2181")
            .load

        originData.registerTempTable("temporaryTable")
        val data=sqlContext.sql("select "+columns+" from temporaryTable")
            .where("OPRATIONFLAG != 'DELETE'")
            .where(dateColumn + ">from_unixtime( to_unix_timestamp(now())-" + seconds + " , 'yyyy-MM-dd HH:mm:ss')")
        
        // 写出处理结果
        val hdfsConf = sc.hadoopConfiguration
        val hdfs = FileSystem.get(new URI("/"), hdfsConf, "hdfs")
        val path = new Path(targetPath)
        if (hdfs.exists(path)) {
            hdfs.delete(path, true)
        }
    
        data.map(row => {
            val str = row.toString
            str.substring(1, str.length() - 1).replaceAll("null","").replaceAll(",","\001")
        }).saveAsTextFile(targetPath)
        
    }
    
}
