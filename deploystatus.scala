package datamarket
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.phoenix.spark._
import java.time.LocalDate
/*
输入日期昨天
计算 昨天的布防状态
统计日期 昨天
 以及
 datawarehouse.fact_deploystatus
 */
object deploystatus {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dmdeploystatus").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    //val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    import hiveContext._

    //val  currentdate = LocalDate.now()
    //val  lastdate=currentdate.minusDays(1)
    val tyyear = args(0)
    val tymonth = args(1)
    val tyday = args(2)
    //System.out.println(s"$tyday")
    val day = tyyear + "-" + tymonth + "-" + tyday
    //println(s"$day")
    val lastday1 = sql(s"select  date_sub(\'$day\',1) ")
      .collect().head.getDate(0)
    //println(s"$lastday1")
    val lastday2=lastday1+" "+"23:59:59"
    println(s"$lastday2")

    sql("use datawarehouse")
    //sql(s" select distinct  cacustomid,customname, caorgid, clientnumber,acttime,originalstatus from fact_deploywithdraw   where  actdate>=date_sub(\'$day\',1)  and  actdate<\'$day\' ")
    hiveContext.sql(s"  select  distinct cloudalarmcustomid,cloudalarmorgid,clientnumber,acttime,originalstatus  from  fact_deploywithdraw    where  actdate=\'$day\'  ")
      .registerTempTable("custom_deploy")


    //sql(s"create table  default.time2d22 as
    //sql(s"select daytime from dict_calendar  where  daytime>=\'$day\'  and  daytime<date_add(\'$day\',1)   ")
    sql(s"select daytime from dict_calendar  where  daytime=date_add(\'$day\',1)    ")
      .registerTempTable("time")


    sql(s"select distinct cloudalarmcustomid,cloudalarmorgid,clientnumber,\'${lastday2}\'   as  acttime,1 as originalstatus from custom_deploy")
      .registerTempTable("prepare")

    sql("select * from custom_deploy  union select * from prepare")
      .registerTempTable("combine")

    //sql("drop table default.big_join")    create table default.big_join as
    sql(s" select  A.cloudalarmcustomid,A.cloudalarmorgid,A.clientnumber,B.daytime as  timenode,max(A.acttime) as acttime from combine A join  time B   where  A.acttime<B.daytime  group by A.cloudalarmcustomid,A.cloudalarmorgid,A.clientnumber,B.daytime ")
      .registerTempTable("big_join")

    //sql("drop table default.select_last_action")   create table default.select_last_action as
    sql(s" select A.cloudalarmcustomid,A.cloudalarmorgid,A.clientnumber,A.timenode,A.acttime,B.originalstatus from  big_join A  inner join combine B  on A.cloudalarmcustomid=B.cloudalarmcustomid  AND A.cloudalarmorgid = B.cloudalarmorgid AND A.clientnumber = B.clientnumber AND A.acttime = B.acttime")
      .registerTempTable("select_last_action")

    //sql("drop table default.E_R")  create table  default.E_R  as
    sql("select cloudalarmcustomid,cloudalarmorgid,timenode,sign(sum(originalstatus-1)) as ext_erflag  from  select_last_action group by cloudalarmcustomid,cloudalarmorgid,timenode")
      .registerTempTable("E_R0")

    //write  into  datawarhouse.fact_deploystatus
    val data0 = hiveContext.sql("select distinct  cloudalarmcustomid,cloudalarmorgid,date_sub(timenode,1)    as timenode,(case  ext_erflag  when  0  then  '布防'  else '撤防'  end)  as  status  from  E_R0 ")
    data0.write.mode("append").saveAsTable("datawarehouse.fact_deploystatus")

    hiveContext.sql(s" 	select   childorg.orgname as  branchnetname,childorg.orgtypename   as  orgtype,custom.customname,  org.OrgName AS parentorgname,childorg.orglevel8,  	childorg.orglevel12," +
      s"  E_R.timenode   as  statisticday,E_R.status " +
      s"  from   fact_deploystatus  E_R   join   dim_organization  childorg  on  E_R.cloudalarmorgid=childorg.cloudalarmorgid   join  dim_custom  custom  on  childorg.cloudalarmcustomid=custom.cloudalarmcustomid " +
      s"  LEFT JOIN  dim_organization  org  on  childorg.cloudalarmparentid=org.cloudalarmorgid   "+
      s"  where  childorg.orglevelcode='04'  ")
      .registerTempTable("deploystatus0")


    hiveContext.sql(s"select   secondlevel.LEVEL,secondlevel.OrgName AS secondorgname  from  dim_organization secondlevel  where  length(secondlevel.`LEVEL`) = 8 ")
      .registerTempTable("secondlevel")

    hiveContext.sql(s"select   thirdlevel.LEVEL,thirdlevel.OrgName AS thirdorgname   from   dim_organization thirdlevel   where  length(thirdlevel.`LEVEL`) = 12 ")
      .registerTempTable("thirdlevel")

    //   (concat(cast(alluser.platformactionid  as varchar),uuid()) )   as rowkey
    hiveContext.sql(s" SELECT     branchnetname,customname,secondorgname,thirdorgname,parentorgname," +
      s"   orgtype,cast(statisticday as string)  as statisticday,status,cast(statisticday  as string)  as  createdate  " +
      s"    from  deploystatus0   LEFT JOIN  secondlevel ON   deploystatus0.orglevel8=secondlevel.LEVEL " +
      s"   LEFT JOIN  thirdlevel ON  deploystatus0.orglevel12=thirdlevel.LEVEL  ")
      .registerTempTable("deploystatus2")





    //val  lastdate=currentdate.minusDays(1)
    //  where   alluser.actiondate>=\'$lastmonday\'  and  alluser.actiondate<=\'$lastsunday\'   ")

    //hiveContext.sql("drop table if  exists   default.deploystatus")
    //hiveContext.sql("create table if not exists  default.deploystatus as  select  * from   deploystatus2 ")


    val data = hiveContext.sql("select  * from  deploystatus2 ")
    data.write.mode("append").saveAsTable("default.deploystatus")


    val dmdf = hiveContext.sql(s" select  ROW_NUMBER() over(order by customname) AS  rowkey,branchnetname,customname,secondorgname,thirdorgname,parentorgname,orgtype,statisticday,status,createdate   from  default.deploystatus  ")
    //.registerTempTable("deploystatus")


    dmdf.write.mode(SaveMode.Overwrite)
      .options(Map("table" -> "operation.deploystatus",
        "zkUrl" -> "192.168.11.24:60002"))
      .format("org.apache.phoenix.spark").save()


    sc.stop




  }
}
