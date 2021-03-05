package com.utils;

/**
 * @Author defei.su
 * @Date 2021/3/5 15:52
 */


import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * UDF(User-Defined-Function) 一进一出
 * UDAF(User-Defined Aggregation -Function)) 聚合函数，多进一出 例如 sum() count() 等
 * UDTF(User-Defined-Table-Generating Function)) 一进多出 例如 Explode(col)  对集合类型进行拆分
 */
public class HiveFuncLow extends UDF {

    /**
     * 1. 创建函数继承 UDF ，实现 evaluate 函数
     * 2. 打jar 包上传至服务器
     * 3. 将 jar 包添加至 Hive||   add jar /opt/module/datas/udf.jar
     * 4. 创建临时函数与上传jar 相关联 ||   create temporary function lower as "com.utils.HiveFuncLow"
     * 5. 即可在HQL中使用自定义函数    ||  " select ename, mylower(ename) lowername from emp"
     */

    public String evaluate (final String s) {
        if (s == null || s.length() == 0 ){
             return null;
        }
        return s.toLowerCase();
    }
}
