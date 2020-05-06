:reset
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "240")
import org.apache.spark.sql.types.{DoubleType, IntegerType}

// 授信用户
val A = spark.sql("select card_no from fbicsi.SOR_EVT_CRT_QUOTA_PRE_CREDIT").
withColumnRenamed("card_no","cust_ids").
filter(length($"cust_ids") === 18).
withColumn("time_censor", from_unixtime(unix_timestamp())).
select("cust_ids","time_censor").distinct()

A.write.mode("overwrite").saveAsTable("usfinance.shichu11_yc_deploy")

//户头号
:reset
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "240")
var A = spark.sql("select * from usfinance.shichu11_yc_deploy")
A = A.join(spark.sql("select id_card,acct_no from finance.mls_member_info_all").
           withColumnRenamed("id_card","cust_ids").
           filter(length($"acct_no") === 19 && $"acct_no".isNotNull),Seq("cust_ids")).distinct()

A.write.mode("overwrite").saveAsTable("usfinance.shichu11_acct_main_table_yc_deploy")



//机器指纹记录
:reset
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "600")
var D = spark.sql("select acct_no, cust_ids, time_censor from usfinance.shichu11_acct_main_table_yc_deploy")
var D1 = spark.sql("select account_no,pay_time,event_code,device_id,city,pay_order_no from fdm_sor.sor_csi_t_machine_finger_record").
  filter($"pay_time".isNotNull && length($"account_no") === 19 && $"account_no".isNotNull).
  withColumnRenamed("account_no","acct_no")
D = D.join(D1,Seq("acct_no")).distinct().filter($"acct_no".isNotNull).
filter($"pay_time" < $"time_censor")
D.write.mode("overwrite").saveAsTable("usfinance.shichu11_machine_finger_table_yc_deploy")

//易付宝交易表
:reset
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "600")
import org.apache.spark.sql.types.{DoubleType, IntegerType}
var A = spark.sql("select buyer_user_no,merchant_order_id,amount,pay_time,pay_order_id from finance.sor_epps_merchant_order").
withColumn("merchant_order_id",$"merchant_order_id".cast("string")).
filter($"amount" < 10000000 && length($"buyer_user_no") === 19 && $"pay_order_id".isNotNull).distinct()
var B = spark.sql("select merchant_order_id,pay_channel_name,pay_type from finance.sor_epps_merchant_order_detail").
withColumn("merchant_order_id",$"merchant_order_id".cast("string")).
withColumn("is_rxd_pay", when($"pay_channel_name".contains("任性贷"),1).otherwise(0))
var D = spark.sql("select * from usfinance.shichu11_acct_main_table_yc_deploy")
D = D.join(A,A("buyer_user_no") === D("acct_no"))
D = D.join(B,Seq("merchant_order_id"))
D = D.filter($"pay_time" < $"time_censor" && $"pay_time".isNotNull)
D.write.mode("overwrite").saveAsTable("usfinance.shichu11_merchant_order_table_yc_deploy")

//支付方式类特征
:reset
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "600")
var A = spark.sql("select * from usfinance.shichu11_merchant_order_table_yc_deploy")
A = A.withColumn("is_credit_pay", when($"pay_type".contains("CREDIT"), 1).otherwise(0)).
withColumn("pay_month", substring($"pay_time", 1, 7)).
withColumn("censor_month",substring($"time_censor",1,7))
var A1 = A.groupBy("cust_ids", "pay_month").
agg(
  countDistinct(when($"is_credit_pay" === 1, $"pay_order_id")).
  alias("monthly_credit_pay_times"),
  countDistinct(when($"is_credit_pay" === 0, $"pay_order_id")).
  alias("monthly_non_credit_pay_times"),
  sum(when($"is_credit_pay" === 1, $"amount").otherwise(0)).
  alias("monthly_credit_pay_amount"),
  sum(when($"is_credit_pay" === 0, $"amount").otherwise(0)).
  alias("monthly_non_credit_pay_amount"),
  min("censor_month").alias("censor_month")
).
groupBy("cust_ids").agg(
  (sum($"monthly_credit_pay_times")/(sum($"monthly_credit_pay_times") + sum($"monthly_non_credit_pay_times"))).
  alias("credit_pay_ratio_all"),
  max($"monthly_credit_pay_times"/($"monthly_credit_pay_times" + $"monthly_non_credit_pay_times")).
  alias("credit_pay_ratio_max"),
  (sum($"monthly_credit_pay_amount")/(sum($"monthly_credit_pay_amount") + sum($"monthly_non_credit_pay_amount"))).
  alias("credit_pay_amount_ratio_all"),
  max($"monthly_credit_pay_amount"/($"monthly_credit_pay_amount" + $"monthly_non_credit_pay_amount")).
  alias("credit_pay_amount_ratio_max"),
  max(when($"pay_month" === $"censor_month",
           $"monthly_credit_pay_amount"/($"monthly_credit_pay_amount" + $"monthly_non_credit_pay_amount")).otherwise(0.0)).
  alias("credit_pay_amount_ratio_this_month")
)
A1.write.mode("overwrite").saveAsTable("usfinance.shichu11_merchant_feature_1_yc_deploy")

//苏宁消费分析, 购买次数, 购买金额
var A2 = A.groupBy("cust_ids").
agg(
  countDistinct($"merchant_order_id").alias("merchant_order_cnt"),
  avg($"amount"/100.0).alias("merchant_order_amt_avg"),
  avg(when($"is_credit_pay" === 1, $"amount"/100.0).otherwise(0)).alias("merchant_order_credit_pay_amt_avg"),
  (sum(when($"pay_channel_name".contains("任性贷"), $"amount"*(0.01)).otherwise(0))/
     (sum($"amount"/100.0)+0.1)).alias("rxd_pay_amt_ratio")
)
A2.write.mode("overwrite").saveAsTable("usfinance.shichu11_merchant_feature_2_yc_deploy")

//机器指纹类代码
:reset
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "600")
import org.apache.spark.sql.types.{DoubleType, IntegerType}
var A = spark.sql("select acct_no, cust_ids, time_censor,pay_time,event_code,device_id"+
" from usfinance.shichu11_machine_finger_table_yc_deploy").distinct().
withColumn("t0",regexp_replace($"pay_time","\\.", "\\-").cast("timestamp").cast(DoubleType)/86400.0).
withColumn("t2",regexp_replace($"time_censor","\\.", "\\-").cast("timestamp").cast(DoubleType)/86400.0).
withColumn("de_id",when($"device_id".isNull,"unknown").otherwise($"device_id")).
filter($"pay_time" < $"time_censor")
A = A.groupBy("cust_ids").agg(
  sum(when($"event_code" === "E0906", 1.0).otherwise(0.0)).alias("E0906"),
  sum(when($"event_code" === "LOGIN_POST", 1.0).otherwise(0.0)).alias("LOGIN_POST"),
  sum(when($"event_code" === "E0506", 1.0).otherwise(0.0)).alias("E0506"),
  sum(when($"event_code" === "E0406", 1.0).otherwise(0.0)).alias("E0406"),
  sum(when($"event_code" === "E0129", 1.0).otherwise(0.0)).alias("E0129"),
  sum(when($"event_code" === "E0418", 1.0).otherwise(0.0)).alias("E0418"),
  sum(when($"event_code" === "E0428", 1.0).otherwise(0.0)).alias("E0428"),
  sum(when($"event_code" === "E0424", 1.0).otherwise(0.0)).alias("E0424"),
  sum(when($"event_code" === "E0425", 1.0).otherwise(0.0)).alias("E0425"),
  sum(when($"event_code" === "E0434", 1.0).otherwise(0.0)).alias("E0434"),
  min("t0").alias("t0"),
  max("t2").alias("t2"),
  collect_list("t0") as("t1_list"),
  collect_list("de_id") as("id_list")
)
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
val check_7_use4_Udf: UserDefinedFunction = udf((t_list: Seq[Double], id_list: Seq[String]) => {

    val timeIdList: Seq[(Double, String)] = t_list.zip(id_list).sortBy(_._1) //time,id
    val unique_id_list: Seq[String] = id_list.distinct
    val uq_cnt: Int = unique_id_list.size
    val latest_time: Array[Double] = Array.fill(uq_cnt) {
        -9999.9
    }
    var temp_id: String = "."
    var gap: Double = 999999.9
    var result: Double = 0.0
    if (uq_cnt >= 4) {
        var i: Int = 0
        while (i < t_list.size && gap > 7.0) {

            temp_id = timeIdList(i)._2

            for (j <- 1 to uq_cnt) {
                if (unique_id_list(j - 1) == temp_id) {
                    latest_time(j - 1) = timeIdList(i)._1
                }
            }
            var sort_latest_time = latest_time.sortWith(_ > _)
            gap = sort_latest_time(0) - sort_latest_time(3)
            i += 1
        }
        if (gap <= 7.0) {
            result = 1.0
        }
    }
    result
})

A = A.withColumn("7days_device_cnt_more_than4",check_7_use4_Udf.apply($"t1_list",$"id_list")).
withColumn("LOGIN_POST_all",$"LOGIN_POST").
withColumn("E0906_all",$"E0906").
withColumn("E0506_avg",$"E0506"/($"t2" - $"t0")).
withColumn("LOGIN_POST_avg",$"LOGIN_POST"/($"t2" - $"t0")).
withColumn("E0406_avg",$"E0406"/($"t2" - $"t0")).
withColumn("E0906_avg",$"E0906"/($"t2" - $"t0")).
withColumn("E0418_avg",$"E0418"/($"t2" - $"t0")).
withColumn("E0129_avg",$"E0129"/($"t2" - $"t0")).
withColumn("E0428_avg",$"E0428"/($"t2" - $"t0")).
withColumn("E0424_avg",$"E0424"/($"t2" - $"t0")).
withColumn("E0425_avg",$"E0425"/($"t2" - $"t0")).
withColumn("E0434_avg",$"E0434"/($"t2" - $"t0")).
select("cust_ids","LOGIN_POST_all","E0906_all","E0506_avg","LOGIN_POST_avg","E0406_avg","E0906_avg",
       "E0418_avg","E0129_avg","E0428_avg","E0424_avg","E0425_avg","E0434_avg","7days_device_cnt_more_than4")
A.write.mode("overwrite").saveAsTable("usfinance.shichu11_machine_finger_feature_yc_deploy")

//机器指纹和订单联合信息
:reset
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "600")
var A = spark.sql("select pay_order_id,is_rxd_pay from usfinance.shichu11_merchant_order_table_yc_deploy").filter($"is_rxd_pay" === 1)
var B = spark.sql("select cust_ids,acct_no,pay_order_no,pay_time,event_code,city,device_id from usfinance.shichu11_machine_finger_table_yc_deploy").
filter($"event_code" === "E0418" || $"event_code" === "E0406").
withColumnRenamed("pay_order_no","pay_order_id")
var D = B.join(A,Seq("pay_order_id"),"left")
D = D.withColumn("pay_month", substring($"pay_time", 1, 7)).
groupBy("cust_ids","acct_no", "pay_month").agg(
countDistinct($"device_id").alias("month_machine_finger_loan_use_device_cnt"),
countDistinct($"city").alias("month_machine_finger_loan_use_city_cnt")).
groupBy("cust_ids").agg(
max("month_machine_finger_loan_use_device_cnt").alias("month_machine_finger_loan_use_device_cnt_max"),
max("month_machine_finger_loan_use_city_cnt").alias("month_machine_finger_loan_use_city_cnt_max"))
D.write.mode("overwrite").saveAsTable("usfinance.shichu11_machine_finger_feature_2_yc_deploy")

//苏宁贷款信息
:reset
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "600")
import org.apache.spark.sql.types.{DoubleType, IntegerType}
var A = spark.sql("select cust_ids,loan_no,loan_time,project_code,loan_amount,partner_no,product_no,quota_type "+
                    "from fbicsi.SOR_EVT_TB_LOAN_MAIN_model").
filter(length($"loan_time") > 5).filter(length($"cust_ids") > 0).
withColumnRenamed("loan_time","loan_apply_time").
withColumnRenamed("loan_amount","loan_apply_amount").
withColumn("is_xfd",when($"partner_no"==="10" && $"product_no"==="RXD" && $"quota_type"==="02",1.0).otherwise(0.0)).
select("cust_ids","loan_no","loan_apply_time","loan_apply_amount","is_xfd")
var od_info = spark.sql("select loan_no,start_date,stat_date from fbicsi.sor_evt_tb_overdue_model_new_model").distinct().
withColumn("etl_time",concat(substring($"stat_date",1,4),lit("-"),
substring($"stat_date",5,2),lit("-"),substring($"stat_date",7,2))).
groupBy("loan_no").agg(
  min($"etl_time").alias("od_rec")
)
var pay_info = spark.sql("select loan_no,status,apply_time from fbicsi.SOR_EVT_TB_repayment_model").
distinct().filter($"loan_no".isNotNull).filter($"apply_time".isNotNull).filter($"status"==="200").
groupBy("loan_no").agg(min("apply_time").alias("payt")).
withColumn("pt",$"payt")
var D = A.join(od_info,Seq("loan_no"),"left")
D = D.join(pay_info,Seq("loan_no"),"left").
withColumnRenamed("loan_no","ln").
withColumn("t1",$"loan_apply_time").
select("cust_ids","od_rec","pt","ln","is_xfd","t1")
A = A.select("cust_ids","loan_no","loan_apply_time").
withColumn("t0",$"loan_apply_time")
var D1 = A.join(D,Seq("cust_ids"),"left").filter($"ln" =!= $"loan_no" && $"t1" < $"t0").
groupBy("cust_ids").agg(
  sum(when(($"od_rec".isNull && $"pt".isNotNull && $"pt" < $"t0") || 
  ($"od_rec".isNotNull && $"od_rec" > $"t0" && $"pt".isNotNull && $"pt" < $"t0"),1.0).otherwise(0.0)).alias("all_good_cnt"),
  sum(when($"od_rec".isNotNull && $"od_rec" < $"t0", 1.0).otherwise(0.0)).alias("all_bad_cnt"),
  sum(when((($"od_rec".isNull && $"pt".isNotNull && $"pt" < $"t0") || 
  ($"od_rec".isNotNull && $"od_rec" > $"t0" && $"pt".isNotNull && $"pt" < $"t0")) && $"is_xfd"===1.0,1.0).
  otherwise(0.0)).alias("xfd_good_cnt"),
  sum(when($"od_rec".isNotNull && $"od_rec" < $"t0" && $"is_xfd"===1.0, 1.0).otherwise(0.0)).alias("xfd_bad_cnt")
)
D1 = D1.withColumn("all_odds_ratio",$"all_bad_cnt"/($"all_good_cnt"+$"all_bad_cnt"+0.1)).
withColumn("xfd_odds_ratio",$"xfd_bad_cnt"/($"xfd_good_cnt"+$"xfd_bad_cnt"+0.1)).distinct()
D1.write.mode("overwrite").saveAsTable("usfinance.shichu11_loan_historical_info_yc_deploy")

//消费贷累计申请和还款信息
:reset
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "600")
import org.apache.spark.sql.types.{DoubleType, IntegerType}
var A = spark.sql("select loan_amount,loan_term,loan_rate,project_code," +
                    "cust_ids,customer_no,loan_no,loan_time,partner_no,product_no,quota_type from fbicsi.SOR_EVT_TB_LOAN_MAIN_model").
filter(length($"loan_time") > 5).filter(length($"cust_ids") > 0).
filter($"partner_no"==="10").filter($"product_no"==="RXD").filter($"quota_type"==="02").select("loan_no","cust_ids").distinct()
var B = spark.sql("select loan_no,principal,interest,status,apply_time,debit_time from fbicsi.SOR_EVT_TB_repayment_model").
distinct().filter($"loan_no".isNotNull).filter($"apply_time".isNotNull).filter($"status"==="200").
withColumn("p1",when($"principal".isNotNull,$"principal").otherwise(0.0)).
withColumn("i1",when($"interest".isNotNull,$"interest").otherwise(0.0)).
withColumn("t1",regexp_replace($"apply_time", "\\.", "\\-").cast("timestamp").cast(DoubleType)/86400.0)
A = B.join(A,Seq("loan_no"),"left").groupBy("cust_ids").agg(
  collect_list("p1") as "p_list",
  collect_list("i1") as "i_list",
  collect_list("t1") as "t_list"
)
var D = spark.sql("select cust_ids, time_censor from usfinance.shichu11_yc_deploy")
D = D.join(A,Seq("cust_ids"),"left").
withColumn("t0",regexp_replace($"time_censor", "\\.", "\\-").cast("timestamp").cast(DoubleType)/86400.0)
def check_amt(arr:Seq[Double],arr_t:Seq[Double],t0:Double):Double={
  var result = 0.0
  var len = arr.size
  if(len>=1){
    for (i <- 1 to len){
      if(arr_t(i-1) < t0){
        result = result + arr(i-1)
      }}}
  result
}
var check_amt_udf = udf(check_amt _)
D = D.withColumn("xfd_int_pay_sum",when($"t_list".isNotNull,check_amt_udf.apply($"i_list",$"t_list",$"t0")).otherwise(0.0)).
withColumn("xfd_p_pay_sum",when($"t_list".isNotNull,check_amt_udf.apply($"p_list",$"t_list",$"t0")).otherwise(0.0)).
select("cust_ids","xfd_int_pay_sum","xfd_p_pay_sum")

var D1 = spark.sql("select cust_ids,time_censor from usfinance.shichu11_yc_deploy")
var A1 = spark.sql("select loan_amount,loan_term,loan_rate,project_code," +
                     "cust_ids,customer_no,loan_no,loan_time,partner_no,product_no,quota_type from fbicsi.SOR_EVT_TB_LOAN_MAIN_model").
filter(length($"loan_time") > 5).filter(length($"cust_ids") > 0).
filter($"partner_no"==="10").filter($"product_no"==="RXD").filter($"quota_type"==="02").
select("cust_ids","loan_time","loan_amount").withColumnRenamed("loan_time","lt").distinct()

D1 = D1.join(A1,Seq("cust_ids"),"left").
withColumn("t0",regexp_replace($"time_censor", "\\.", "\\-").cast("timestamp").cast(DoubleType)/86400.0).
withColumn("t1",regexp_replace($"lt", "\\.", "\\-").cast("timestamp").cast(DoubleType)/86400.0).
withColumn("amt",when($"t1" < $"t0",$"loan_amount").otherwise(0.0)).
groupBy("cust_ids").agg(sum("amt").alias("xfd_apply_amt_cum"))

var D2 = D1.join(D,Seq("cust_ids"),"left").withColumn("xfd_principal_pay_ratio",
 when($"xfd_apply_amt_cum">0.0,$"xfd_p_pay_sum"/$"xfd_apply_amt_cum").otherwise(-0.1)).distinct()
D2.write.mode("overwrite").saveAsTable("usfinance.shichu11_loan_xfd_info_yc_deploy")


//任性贷还款特征
:reset
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "600")
var D = spark.sql("select loan_no,cust_ids,loan_time from fbicsi.SOR_EVT_TB_LOAN_MAIN_model").distinct().
filter(length($"loan_time") > 5).filter(length($"cust_ids") > 0).
withColumn("lt",$"loan_time")
var A = spark.sql("select loan_no,principal,status,apply_time from fbicsi.SOR_EVT_TB_repayment_model").
distinct().filter(length($"apply_time")>10).filter($"status".isNotNull).filter($"status"==="200" || $"status"==="500").
withColumn("p_amt",when($"principal".isNotNull,$"principal").otherwise(0.0)).
withColumn("pt",$"apply_time")
D = D.join(A,Seq("loan_no"),"left").select("cust_ids","lt","status","p_amt","pt")
var D1 = spark.sql("select cust_ids,time_censor from usfinance.shichu11_yc_deploy").
withColumn("lt0",$"time_censor")
D1 = D1.join(D,Seq("cust_ids"),"left").filter($"lt" < $"lt0" && $"pt" < $"lt0").groupBy("cust_ids").agg(
  sum(when($"status"==="200",1.0).otherwise(0.0)).alias("suc_cnt"),
  sum(when($"status"==="500",1.0).otherwise(0.0)).alias("fail_cnt"),
  sum(when($"status"==="200",$"p_amt").otherwise(0.0)).alias("p_sum")
).withColumn("s_f_rate",$"fail_cnt"/($"fail_cnt"+$"suc_cnt"+0.01)).
withColumn("rxd_principal_pay_cum",when($"p_sum".isNotNull,$"p_sum").otherwise(0.0))
D1.write.mode("overwrite").saveAsTable("usfinance.shichu11_rxd_pay_info_yc_deploy")

//人行特征
//一代人行特征
:reset
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "600")
var D = spark.sql("select cust_ids,time_censor from usfinance.shichu11_yc_deploy")
var A = spark.sql("select report_sn,cert_no,report_create_time from fbicsi.SOR_EVT_TBL_PER_RESULT_INFO_model").distinct()
D = D.join(A,D("cust_ids") === A("cert_no"),"left").filter($"report_create_time" < $"time_censor").
select("cert_no","report_sn","report_create_time","cust_ids")
var A1 = D.groupBy("cust_ids").agg(max("report_create_time").alias("t0"))
D = D.join(A1,Seq("cust_ids"),"left").
filter($"report_create_time" === $"t0" && $"report_create_time".isNotNull && $"t0".isNotNull)
var A2 = D.groupBy("cust_ids").agg(max("report_sn").alias("id"))
D = D.join(A2,Seq("cust_ids"),"left").
filter($"report_sn" === $"id" && $"report_sn".isNotNull && $"id".isNotNull).
select("cust_ids","cert_no","report_sn","report_create_time")
D.write.mode("overwrite").saveAsTable("usfinance.rxd_shichu11_renhang1_yc_deploy")


:reset
var A = spark.sql("select * from usfinance.rxd_shichu11_renhang1_yc_deploy")
var B = spark.sql("select report_id,querydate,queryreason from fbicsi.SOR_EVT_TBL_PER_RECORDDETAIL_model")
var B1 = spark.sql("select report_id,LASTMONTHQUERY_SELFQUERY,LAST2YEARQUERY_LOANMANAGE from "+
"fbicsi.SOR_EVT_TBL_PER_QUERYRECORDCOLLECT_model").distinct()
var D = A.join(B,A("report_sn")===B("report_id"),"left").
withColumn("no_days",datediff(to_date(regexp_replace(substring($"report_create_time",1,10), lit("\\."), lit("\\-"))), 
                              to_date(regexp_replace(substring($"querydate",1,10), lit("\\."), lit("\\-")))))
D = D.join(B1,D("report_sn")===B1("report_id"),"left")
D = D.groupBy("cust_ids").agg(
  sum(when($"no_days"<=90,1.0).otherwise(0.0)).alias("query_cnt_3m_1"),
  sum(when($"no_days"<=180,1.0).otherwise(0.0)).alias("query_cnt_6m_1"),
  sum(when($"no_days"<=365,1.0).otherwise(0.0)).alias("query_cnt_12m_1"),
  sum(when($"no_days"<=90 && $"queryreason" === "贷款审批",1.0).otherwise(0.0)).alias("query_loan_cnt_3m_1"),
  sum(when($"no_days"<=180 && $"queryreason" === "贷款审批",1.0).otherwise(0.0)).alias("query_loan_cnt_6m_1"),
  sum(when($"no_days"<=365 && $"queryreason" === "贷款审批",1.0).otherwise(0.0)).alias("query_loan_cnt_12m_1"),
  sum(when($"no_days"<=90 && $"queryreason" === "信用卡审批",1.0).otherwise(0.0)).alias("query_card_cnt_3m_1"),
  sum(when($"no_days"<=365 && $"queryreason" === "信用卡审批",1.0).otherwise(0.0)).alias("query_card_cnt_12m_1"), 
  max("LAST2YEARQUERY_LOANMANAGE").alias("query_loanmanage_24m_1"),
  max("LASTMONTHQUERY_SELFQUERY").alias("query_selfquery_1m_1"),
  countDistinct("LASTMONTHQUERY_SELFQUERY").alias("uq")
)
D.write.mode("overwrite").saveAsTable("usfinance.shichu11_rh1_yc_info_deploy")

//人行二代
:reset
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "600")
val label = spark.sql("select cust_ids,time_censor from usfinance.shichu11_yc_deploy")

val main_report = spark.sql("select id, report_code, create_time from fbicsi.T_BICDT_TPQR_DOCUMENT_D where create_time > '2020-01-20'").distinct()
//val A1 = A.groupBy("id").agg(max("create_time").alias("t_max"))
//val main_report = A.join(A1, Seq("id")).filter($"create_time" === $"t_max").dropDuplicates("id")

// 查询记录概要表（query_summary) -- 172310 count
//:reset
val A = spark.sql("select id, doc_id from fbicsi.T_BICDT_TPQR_PQO_D_model").distinct
val A1 = spark.sql("select id, pqo_id from fbicsi.T_BICDT_TPQR_PQO_PC05_D_model").distinct
val A2 = spark.sql("select * from fbicsi.T_BICDT_TPQR_PQO_PC05A_D_model").distinct.
withColumnRenamed("pc05aq01","last_query_reason").
withColumnRenamed("pc05ar01","last_query_date").
withColumnRenamed("pc05ad01","last_query_org").
withColumnRenamed("pc05ai01","last_query_org_code").
select("id",
"pqo_pc05_id",
"last_query_date",
"last_query_reason",
"last_query_date",
"last_query_org",
"last_query_org_code")

val A3 = spark.sql("select * from fbicsi.T_BICDT_TPQR_PQO_PC05B_D_model").distinct.
withColumnRenamed("pc05bs01","query_loan_org_cnt_1m").
withColumnRenamed("pc05bs02","query_card_org_cnt_1m").
withColumnRenamed("pc05bs03","query_loan_cnt_1m").
withColumnRenamed("pc05bs04","query_card_cnt_1m").
withColumnRenamed("pc05bs05","query_selfquery_cnt_1m").
withColumnRenamed("pc05bs06","query_loanmanage_24m").
select("id","pqo_pc05_id","query_loan_org_cnt_1m",
"query_card_org_cnt_1m",
"query_loan_cnt_1m",
"query_card_cnt_1m",
"query_selfquery_cnt_1m",
"query_loanmanage_24m"
)
val query_summary = A.join(A1,A.col("id") === A1.col("pqo_id"),"left").
  join(A2,A1.col("id") === A2.col("pqo_pc05_id"),"left").
  join(A3,A1.col("id") === A3.col("pqo_pc05_id"),"left").
  select("doc_id",
"last_query_reason",
"last_query_date",
"last_query_org",
"last_query_org_code",
"query_loan_org_cnt_1m",
"query_card_org_cnt_1m",
"query_loan_cnt_1m",
"query_card_cnt_1m",
"query_selfquery_cnt_1m",
"query_loanmanage_24m"
).distinct

val A = sql("select * from fbicsi.T_BICDT_TPQR_POQ_D_model").distinct
val A1 = sql("select * from fbicsi.T_BICDT_TPQR_POQ_PH01_D_model").
  withColumnRenamed("ph010r01","query_date").
  withColumnRenamed("ph010d01","management_org_type").
  withColumnRenamed("ph010q02","query_org").
  withColumnRenamed("ph010q03","query_reason").distinct
val query_record = A.join(A1,A.col("id") === A1.col("poq_id"),"left").
  select("doc_id",
"query_date",
"management_org_type",
"query_org",
"query_reason"
)

// 身份证信息
val A = sql("select * from fbicsi.T_BICDT_TPQR_PRH_D_model").distinct
val A1 = sql("select * from fbicsi.T_BICDT_TPQR_PRH_PA01_D_model").
withColumnRenamed("id","report_unit_id").distinct
val A2 = sql("select * from fbicsi.T_BICDT_TPQR_PRH_PA01B_D_model").
withColumnRenamed("pa01bq01","name").
withColumnRenamed("pa01bd01","id_type").
withColumnRenamed("pa01bi01","id_card_num").
withColumnRenamed("pa01bi02","query_ord_code").
withColumnRenamed("pa01bd02","query_reason_code").distinct

val report_query = A.join(A1, A.col("id") === A1.col("prh_id"),"left").
  join(A2, A1.col("report_unit_id") === A2.col("prh_pa01_id"),"left").
  select("doc_id",
"report_unit_id",
"name",
"id_type",
"id_card_num",
"query_ord_code",
"query_reason_code"
).distinct

val renhang2 = main_report.join(query_summary, main_report.col("id") === query_summary.col("doc_id"),"left").
  join(query_record, main_report.col("id") === query_record.col("doc_id"),"left").
  join(report_query,main_report.col("id") === report_query.col("doc_id"),"left").
  select("create_time",
"name",
"id_type",
"id_card_num",
"query_ord_code",
"query_reason_code",
"query_date",
"management_org_type",
"query_org",
"query_reason",
"last_query_reason",
"last_query_date",
"last_query_org",
"last_query_org_code",
"query_loan_org_cnt_1m",
"query_card_org_cnt_1m",
"query_loan_cnt_1m",
"query_card_cnt_1m",
"query_selfquery_cnt_1m",
"query_loanmanage_24m"
).distinct

val renhang2_with_label = label.join(renhang2, label.col("cust_ids") ===  renhang2.col("id_card_num"),"left").
  filter($"create_time" < $"time_censor")

val time_max = renhang2_with_label.groupBy("cust_ids").agg(max("create_time").alias("t0"))
val renhang2_with_label2 = renhang2_with_label.join(time_max, Seq("cust_ids"),"left").
  filter($"create_time" === $"t0" && $"create_time".isNotNull && $"t0".isNotNull).
  withColumn("no_days",datediff(to_date(regexp_replace(substring($"create_time",1,10), lit("\\."), lit("\\-"))),
to_date(regexp_replace(substring($"query_date",1,10),lit("\\."), lit("\\-"))))).
groupBy("cust_ids").agg(
sum(when($"no_days"<=90,1.0).otherwise(0.0)).alias("query_cnt_3m_2"),
sum(when($"no_days"<=180,1.0).otherwise(0.0)).alias("query_cnt_6m_2"),
sum(when($"no_days"<=365,1.0).otherwise(0.0)).alias("query_cnt_12m_2"),
sum(when($"no_days"<=90 && $"query_reason" === 2,1.0).otherwise(0.0)).alias("query_loan_cnt_3m_2"),
sum(when($"no_days"<=180 && $"query_reason" === 2,1.0).otherwise(0.0)).alias("query_loan_cnt_6m_2"),
sum(when($"no_days"<=365 && $"query_reason" === 2,1.0).otherwise(0.0)).alias("query_loan_cnt_12m_2"),
sum(when($"no_days"<=90 && $"query_reason" === 3,1.0).otherwise(0.0)).alias("query_card_cnt_3m_2"),
sum(when($"no_days"<=365 && $"query_reason" === 3,1.0).otherwise(0.0)).alias("query_card_cnt_12m_2"),
max("query_loanmanage_24m").alias("query_loanmanage_24m_2"),
max("query_selfquery_cnt_1m").alias("query_selfquery_cnt_1m_2")
).distinct
renhang2_with_label2.write.mode("overwrite").saveAsTable("usfinance.shichu11_rh2_yc_info_deploy")

//融合一二代人行信息
:reset
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "240")

val label = spark.sql("select cust_ids,time_censor from usfinance.shichu11_yc_deploy")
val rh1 = spark.sql("select * from usfinance.shichu11_rh1_yc_info_deploy")
val rh2 = spark.sql("select * from usfinance.shichu11_rh2_yc_info_deploy")

val rh = label.join(rh1, Seq("cust_ids"),"left").
  join(rh2, Seq("cust_ids"),"left")

val rh_all = rh.withColumn("query_cnt_3m",when($"query_cnt_3m_2".isNotNull, $"query_cnt_3m_2").otherwise($"query_cnt_3m_1")).
  withColumn("query_cnt_6m",when($"query_cnt_6m_2".isNotNull, $"query_cnt_6m_2").otherwise($"query_cnt_6m_1")).
  withColumn("query_cnt_12m",when($"query_cnt_12m_2".isNotNull, $"query_cnt_12m_2").otherwise($"query_cnt_12m_1")).
  withColumn("query_loan_cnt_3m",when($"query_loan_cnt_3m_2".isNotNull, $"query_loan_cnt_3m_2").otherwise($"query_loan_cnt_3m_1")).
  withColumn("query_loan_cnt_6m",when($"query_loan_cnt_6m_2".isNotNull, $"query_loan_cnt_6m_2").otherwise($"query_loan_cnt_6m_1")).
  withColumn("query_loan_cnt_12m",when($"query_loan_cnt_12m_2".isNotNull, $"query_loan_cnt_12m_2").otherwise($"query_loan_cnt_12m_1")).
  withColumn("query_card_cnt_3m",when($"query_card_cnt_3m_2".isNotNull, $"query_card_cnt_3m_2").otherwise($"query_card_cnt_3m_1")).
  withColumn("query_card_cnt_12m",when($"query_card_cnt_12m_2".isNotNull, $"query_card_cnt_12m_2").otherwise($"query_card_cnt_12m_1")).
  withColumn("query_loanmanage_24m",when($"query_loanmanage_24m_2".isNotNull, $"query_loanmanage_24m_2").otherwise($"query_loanmanage_24m_1")).
  withColumn("query_selfquery_1m",when($"query_selfquery_cnt_1m_2".isNotNull, $"query_selfquery_cnt_1m_2").otherwise($"query_selfquery_1m_1")).
  select(
  "cust_ids",
  "time_censor",
  "query_cnt_3m",
  "query_cnt_6m",
  "query_cnt_12m",
  "query_loan_cnt_3m",
  "query_loan_cnt_6m",
  "query_loan_cnt_12m",
  "query_card_cnt_3m",
  "query_card_cnt_12m",
  "query_loanmanage_24m",
  "query_selfquery_1m",
  "uq"
  )
  
rh_all.write.mode("overwrite").saveAsTable("usfinance.shichu11_rh_yc_info_deploy")


//增补人行信息 ----- 只有人行1代的
:reset
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "600")
import org.apache.spark.sql.types.{DoubleType, IntegerType}
var D = spark.sql("select cust_ids,time_censor from usfinance.shichu11_yc_deploy")
var A = spark.sql("select cert_no, report_create_time from fbicsi.SOR_EVT_TBL_PER_RESULT_INFO_model").distinct().
withColumn("t1",regexp_replace($"report_create_time","\\.", "\\-"))
D = D.join(A,A("cert_no")===D("cust_ids"),"left").
withColumn("lt",substring($"time_censor",1,19)).withColumn("rt",substring($"t1",1,19)).
filter($"rt" < $"lt").groupBy("cust_ids").agg(
  max("t1").alias("rt"),
  max("cert_no").alias("cert_no"),
  max("time_censor").alias("time_censor")
)
var B = spark.sql("select cert_no, report_create_time,report_sn from fbicsi.SOR_EVT_TBL_PER_RESULT_INFO_model").distinct().
withColumn("t2",regexp_replace($"report_create_time","\\.", "\\-"))
D = D.join(B,Seq("cert_no"),"left").filter($"t2" === $"rt").groupBy("cust_ids").agg(
  max("report_sn").alias("report_sn"),
  max("cert_no").alias("cert_no"),
  max("time_censor").alias("censor"),
  max("rt").alias("report_create_time")
)
var credit_loan = spark.sql("select * from fbicsi.SOR_EVT_TBL_PER_LOAN_INFO_CUE_model").distinct().
withColumnRenamed("REPORT_ID", "REPORT_SN")
var D1 = D.join(credit_loan,Seq("REPORT_SN"),"left").
select("cust_ids", "LOAN_ID", "START_DATE", "ORG_TYPE", "ORGANNAME", "LOAN_AMOUNT", "LOAN_TYPE",
       "PERIOD", "PAYMENT_METHOD", "END_DATE", "ACCOUNT_STATUS","censor").
withColumn("LOAN_DAYS", datediff(to_date(regexp_replace($"end_date", lit("\\."), lit("\\-"))), 
                                 to_date(regexp_replace($"start_date", lit("\\."), lit("\\-"))))).
filter(to_date(regexp_replace($"START_DATE", lit("\\."), lit("\\-"))) >= date_sub(to_date(substring($"censor",1,10)),365) &&
$"LOAN_TYPE" =!= "个人汽车贷款" && $"LOAN_TYPE" =!= "个人商用房（包括商住两用）贷款" && $"LOAN_TYPE" =!= "个人住房贷款").distinct()
var credit_settled_analysis = D1.
groupBy("cust_ids").agg(
  countDistinct(when($"ACCOUNT_STATUS" === "结清", $"loan_id")).alias("settled_loan_cnt_1y"),
  countDistinct(when($"ACCOUNT_STATUS" =!= "结清", $"loan_id")).alias("not_settled_loan_cnt_1y"),
  sum(when($"ACCOUNT_STATUS" === "结清", $"LOAN_AMOUNT").otherwise(0)).alias("settled_loan_amount_1y"),
  sum(when($"ACCOUNT_STATUS".isNull && to_date(regexp_replace($"end_date", lit("\\."), lit("\\-"))) > to_date(substring($"censor",1,10)),
  ($"LOAN_AMOUNT"/$"PERIOD") *((datediff(regexp_replace($"end_date", lit("\\."), lit("\\-")), 
  to_date(substring($"censor",1,10)))/$"LOAN_DAYS")*$"PERIOD").cast(IntegerType)).
  otherwise(0.0)).alias("not_settled_loan_paid_amount_1y"),
  sum(when($"ACCOUNT_STATUS".isNull, $"LOAN_AMOUNT").otherwise(0)).alias("not_settled_loan_amount_1y"),
  sum($"LOAN_AMOUNT").alias("loan_amount_1y"),
  min($"censor").alias("censor")
).
withColumn("settled_loan_cnt_ratio_1y_a", $"settled_loan_cnt_1y"/($"settled_loan_cnt_1y" + $"not_settled_loan_cnt_1y")).
withColumn("settled_loan_cnt_ratio_1y", when($"settled_loan_cnt_ratio_1y_a".isNull,0.0).otherwise($"settled_loan_cnt_ratio_1y_a")).
withColumn("settled_loan_amount_ratio_1y", ($"settled_loan_amount_1y" + $"not_settled_loan_paid_amount_1y")/$"loan_amount_1y").
select("cust_ids", "settled_loan_cnt_ratio_1y", "settled_loan_amount_ratio_1y")
credit_settled_analysis.write.mode("overwrite").saveAsTable("usfinance.shichu11_rh_feature_2_yc_deploy")

//办理三张信用卡用的最短时间 是否小于30天 
import org.apache.spark.sql.expressions.Window
var w6 = Window.partitionBy($"cust_ids").orderBy($"start_date".asc)
var loan_card_info_analysis = spark.sql("select * from fbicsi.SOR_EVT_TBL_PER_LOAN_CARD_INFO_CUE_model").
withColumnRenamed("REPORT_ID", "REPORT_SN")
loan_card_info_analysis = D.join(loan_card_info_analysis, Seq("REPORT_SN"),"left").
select("cust_ids", "START_DATE", "ORG_TYPE", "ORGANNAME",
       "CARD_TYPE", "guarantee_type", "deadline","censor").distinct().
withColumn("lag2_loan_card_time", lag($"start_date", 2).over(w6)).
withColumn("lag2_loan_card_time_diff",datediff(to_date(regexp_replace($"START_DATE", lit("\\."), 
lit("\\-"))),to_date(regexp_replace($"lag2_loan_card_time", lit("\\."), lit("\\-"))))).
withColumn("censor_date",when(!($"censor".isNull),$"censor").otherwise(current_date())).
groupBy("cust_ids").agg(min($"lag2_loan_card_time_diff").alias("lag2_loan_card_time_diff_min"))
loan_card_info_analysis.write.mode("overwrite").saveAsTable("usfinance.shichu11_rh_feature_3_yc_deploy")

//其他贷款逾期超过3天(超过10天的太少)
:reset
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "240")
import org.apache.spark.sql.types.{DoubleType, IntegerType}
var A = spark.sql("select cust_ids,time_censor from usfinance.shichu11_yc_deploy").
withColumn("t0",regexp_replace($"time_censor","\\.", "\\-").cast("timestamp").cast(DoubleType)/86400.0)
var B = spark.sql("select project_code,cust_ids,loan_no,loan_time,partner_no,product_no,quota_type"+
" from fbicsi.SOR_EVT_TB_LOAN_MAIN_model").distinct().
filter(length($"loan_time") > 5).filter(length($"cust_ids") > 0).
filter($"partner_no"=!="10" || $"product_no"=!="RXD" || $"quota_type"=!="02").
withColumnRenamed("loan_time","loan_apply_time").select("loan_no","cust_ids").distinct()
var D = spark.sql("select start_date, loan_no, overdue_days from fbicsi.sor_evt_tb_overdue_model_new_model").distinct().
withColumn("t1_c",concat(substring($"start_date",1,10),lit(" "),lit("00:00:00"))).
withColumn("t1",regexp_replace($"t1_c","\\.", "\\-").cast("timestamp").cast(DoubleType)/86400.0).
filter($"overdue_days">3.0).withColumn("rec",lit(1.0))
B = B.join(D,Seq("loan_no")).drop("loan_no")
var od_table1 = A.join(B,Seq("cust_ids"),"left").filter($"t1" < $"t0" - 3.0).groupBy("cust_ids").agg(max("rec").alias("od10_dummy"))

var A1 = spark.sql("select cust_ids,time_censor from usfinance.shichu11_yc_deploy").
withColumn("t0",substring($"time_censor",1,10))
var B1 = spark.sql("select project_code,cust_ids,loan_no,loan_time,partner_no,product_no,quota_type"+
" from fbicsi.SOR_EVT_TB_LOAN_MAIN_model").distinct().
filter(length($"loan_time") > 5).filter(length($"cust_ids") > 0).
withColumnRenamed("loan_time","loan_apply_time").select("loan_no","cust_ids").distinct()
var D1 = spark.sql("select stat_date, loan_no, overdue_days from fbicsi.sor_evt_tb_overdue_model_new_model").distinct().
withColumn("t1",concat(substring($"stat_date",1,4),lit("-"),
substring($"stat_date",5,2),lit("-"),substring($"stat_date",7,2))).
filter($"overdue_days">0.0).withColumn("rec",lit(1.0))
B1 = B1.join(D1,Seq("loan_no")).drop("loan_no")
var od_table2 = A1.join(B1,Seq("cust_ids"),"left").filter($"t1" < $"t0").
groupBy("cust_ids").agg(max("rec").alias("overdue_record_new_dummy"))


//累计消费贷次数以及上一笔信息
var xfd_table = spark.sql("select loan_amount,loan_term,loan_rate,project_code," +
"cust_ids,customer_no,loan_no,loan_time,partner_no,product_no,quota_type from fbicsi.SOR_EVT_TB_LOAN_MAIN_model").
filter(length($"loan_time") > 5).filter(length($"cust_ids") > 0).
filter($"partner_no"==="10").filter($"product_no"==="RXD").filter($"quota_type"==="02").
select("loan_no","cust_ids","loan_time","loan_term","loan_rate","loan_amount").withColumnRenamed("loan_no","ln").distinct()
var B2 = spark.sql("select * from usfinance.shichu11_yc_deploy")
B2 = B2.join(xfd_table,Seq("cust_ids"),"left").filter($"loan_time" < $"time_censor")
B2 = B2.groupBy("cust_ids").agg(
  count(lit(1)).alias("cnt"),
  max("loan_time").alias("last_t")
)
var D2 = B2.join(xfd_table,Seq("cust_ids"),"left").filter($"last_t" === $"loan_time").
withColumn("last_apply_term",$"loan_term".cast(DoubleType)).
withColumn("last_apply_rate",$"loan_rate".cast(DoubleType)).
withColumn("last_apply_amount",$"loan_amount".cast(DoubleType)).
withColumn("all_xfd_loan_cnt",$"cnt".cast(DoubleType)).
select("cust_ids","all_xfd_loan_cnt","last_apply_amount","last_apply_rate","last_apply_term")

//整合特征
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "240")
var feature_table = spark.sql("select * from usfinance.shichu11_yc_deploy").
withColumn("t0",regexp_replace($"time_censor","\\.", "\\-").cast("timestamp").cast(DoubleType)/86400.0)

feature_table = feature_table.join(spark.sql("select * from usfinance.shichu11_merchant_feature_1_yc_deploy"),Seq("cust_ids"),"left")
feature_table = feature_table.join(spark.sql("select * from usfinance.shichu11_merchant_feature_2_yc_deploy"),Seq("cust_ids"),"left").
withColumnRenamed("merchant_order_cnt","x").withColumn("merchant_order_cnt",$"x".cast(DoubleType)).drop("x")
feature_table = feature_table.join(spark.sql("select * from usfinance.shichu11_machine_finger_feature_yc_deploy"),Seq("cust_ids"),"left")
feature_table = feature_table.join(spark.sql("select * from usfinance.shichu11_loan_historical_info_yc_deploy"),Seq("cust_ids"),"left")
feature_table = feature_table.join(spark.sql("select * from usfinance.shichu11_rh_yc_info_deploy").drop("time_censor"),Seq("cust_ids"),"left")
feature_table = feature_table.join(spark.sql("select * from usfinance.shichu11_loan_xfd_info_yc_deploy"),Seq("cust_ids"),"left").drop("xfd_p_pay_sum")
feature_table = feature_table.join(spark.sql("select * from usfinance.shichu11_rh_feature_2_yc_deploy"),Seq("cust_ids"),"left")
feature_table = feature_table.join(spark.sql("select * from usfinance.shichu11_rh_feature_3_yc_deploy"),Seq("cust_ids"),"left")
feature_table = feature_table.join(spark.sql("select * from usfinance.shichu11_machine_finger_feature_2_yc_deploy"),Seq("cust_ids"),"left")
feature_table = feature_table.join(od_table1,Seq("cust_ids"),"left")
feature_table = feature_table.join(od_table2,Seq("cust_ids"),"left")
feature_table = feature_table.join(D2,Seq("cust_ids"),"left").
withColumnRenamed("all_xfd_loan_cnt","all_xfd_loan_cnt_0").
withColumn("all_xfd_loan_cnt",when($"all_xfd_loan_cnt_0".isNull,1.0).otherwise($"all_xfd_loan_cnt_0"+1.0)).
drop("all_xfd_loan_cnt_0")

feature_table.write.mode("overwrite").saveAsTable("usfinance.shichu11_feature_table_tmp_yc_deploy")

:reset
import org.apache.spark.sql.types.{DoubleType, IntegerType}
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "600")
var feature_table = spark.sql("select * from usfinance.shichu11_feature_table_tmp_yc_deploy")
val pay_info = spark.sql("select * from usfinance.shichu11_rxd_pay_info_yc_deploy")
feature_table = feature_table.join(pay_info,Seq("cust_ids"),"left").
withColumn("dow",$"t0".cast("Int") % 7).withColumn("yr",substring($"time_censor",1,4).cast(DoubleType)).
withColumn("id_num",substring($"cust_ids",1,17).cast(DoubleType)).withColumn("id_head_1",substring($"cust_ids",3,2).cast(DoubleType)).
withColumn("hr",substring($"time_censor",12,2).cast(DoubleType)).
withColumn("age",substring($"cust_ids",7,4).cast(DoubleType)*(-1.0)+$"yr").
withColumn("good_ratio",$"xfd_good_cnt"/($"all_xfd_loan_cnt"+0.0)).withColumn("bad_ratio",$"xfd_bad_cnt"/($"all_xfd_loan_cnt"+0.0)).
withColumnRenamed("lag2_loan_card_time_diff_min","tmp1").
withColumn("lag2_loan_card_time_diff_min",$"tmp1".cast(DoubleType)).drop("tmp1").
withColumnRenamed("dow","tmp1").withColumn("dow",$"tmp1".cast(DoubleType)).drop("tmp1").
withColumnRenamed("query_loanmanage_24m","tmp1").withColumn("query_loanmanage_24m",$"tmp1".cast(DoubleType)).drop("tmp1").
withColumnRenamed("query_selfquery_1m","tmp1").withColumn("query_selfquery_1m",$"tmp1".cast(DoubleType)).drop("tmp1").
drop("uq","t0","yr")

feature_table.write.mode("overwrite").saveAsTable("usfinance.shichu11_lab_data_tmp_yc_deploy")

// 实时特征
:reset
import org.apache.spark.sql.types.{DoubleType, IntegerType}
val feature_table = spark.sql("select * from usfinance.shichu11_lab_data_tmp_yc_deploy")
val B = spark.sql("select cust_ids,loan_amount, loan_no,loan_time,loan_term,"+
"partner_no,product_no, quota_type from fbicsi.SOR_EVT_TB_LOAN_MAIN_model").
filter($"partner_no"==="10").filter($"product_no"==="RXD").filter($"quota_type"==="02").
withColumn("loan_apply_amount",$"loan_amount".cast(DoubleType)). 
withColumn("loan_apply_rate",lit(null)).withColumn("loan_apply_rate",$"loan_apply_rate".cast(DoubleType)).
withColumn("loan_apply_term",$"loan_term".cast(DoubleType)).
withColumn("loan_apply_time",$"loan_time").
select("cust_ids","loan_no","loan_apply_amount","loan_apply_rate","loan_apply_term","loan_apply_time")
val all_features = feature_table.join(B, Seq("cust_ids"),"left")
all_features.write.mode("overwrite").saveAsTable("usfinance.shichu11_lab_data_tmp_yc2_deploy")

//将授信信息加入
:reset
import org.apache.spark.sql.types.{DoubleType, IntegerType}
spark.sqlContext.setConf("spark.sql.shuffle.partitions", "240")
var A = spark.sql("select * from usfinance.SOR_EVT_CRT_QUOTA_PRE_CREDIT").distinct().
withColumnRenamed("card_no","cust_ids")
var B = spark.sql("select * from usfinance.shichu11_lab_data_tmp_yc_deploy")
B = B.join(A,Seq("cust_ids"),"left").
withColumn("t0",regexp_replace($"time_censor","\\.", "\\-").cast("timestamp").cast(DoubleType)/86400.0).
withColumn("t1",regexp_replace($"credit_date","\\.", "\\-").cast("timestamp").cast(DoubleType)/86400.0).
withColumn("credit_age",when($"t1" < $"t0" && $"t1".isNotNull && $"t0".isNotNull,$"t0" - $"t1").otherwise(null)).
drop("t0","t1","credit_date")
var D = spark.sql("select card_no,credit_date from fbicsi.SOR_EVT_CRT_QUOTA_PRE_CREDIT").distinct().
filter(length($"card_no")===18).withColumnRenamed("card_no","cust_ids").
groupBy("cust_ids").agg(min("credit_date").alias("credit_date"))
B = B.join(D,Seq("cust_ids"),"left").
withColumn("t0",regexp_replace($"time_censor","\\.", "\\-").cast("timestamp").cast(DoubleType)/86400.0).
withColumn("t1",regexp_replace($"credit_date","\\.", "\\-").cast("timestamp").cast(DoubleType)/86400.0).
withColumn("credit_age_new",when($"t1" < $"t0" && $"t1".isNotNull && $"t0".isNotNull,$"t0" - $"t1").otherwise(null)).
drop("t0","t1","credit_date").
withColumn("credit_age",when($"credit_age".isNotNull,$"credit_age").otherwise($"credit_age_new")).drop("credit_age_new")
B.write.mode("overwrite").saveAsTable("usfinance.shichu11_lab_data_yc_deploy")




