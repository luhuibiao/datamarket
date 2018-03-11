package datamarket
/*
±¨¾¯»Ø¸´
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.phoenix.spark._
import java.time.LocalDate
object alarmreply {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dmalarmreply").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //val  currentdate= LocalDate.now()
    val tyyear=args(0)
    val tymonth=args(1)
    val tyday=args(2)
    //System.out.println(s"$tyday")
    val  day=tyyear+"-"+tymonth+"-"+tyday
    //println(s"$day")
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    //hive
    hiveContext.sql("use datawarehouse")
    hiveContext.sql(s"  select  distinct  dworgid,codeid,clientnumber,zoneid,alarmtime,alarmcreatetime,dwuserid,replymemo,replytime  from  fact_alarm    where   to_date(alarmcreatetime)=\'$day\'  ")
      .registerTempTable("factalarmtemp")


    hiveContext.sql(s" 	select  distinct childorg.orgname as  branchnetname,  host.clientnumber   as   hostnumber,sector.name  as  sectorname, " +
      s"  childorg.orgtypename   as  orgtype,custom.customname,  org.OrgName AS parentorgname,childorg.orglevel8,  	childorg.orglevel12,alarmtype.codename,1 as alarmnumber,"+
      s" alarm.alarmtime,to_date(alarm.alarmcreatetime) as  createdate,"+
      s" user2.name as replyusername,post2.postname,role2.rolename,userorg.orgname as replyuserorgname,replymemo as  replycontent,replytime  " +
      s" from  factalarmtemp  alarm   join   dim_organization  childorg  on  alarm.dworgid=childorg.dworgid    join  dim_custom  custom  on  childorg.cloudalarmcustomid=custom.cloudalarmcustomid " +
      s"  LEFT JOIN dim_organization org  on  childorg.cloudalarmparentid=org.cloudalarmorgid   left    join     dim_host  host   on  alarm.clientnumber=host.clientnumber    and      childorg.cloudalarmorgid=host.cloudalarmorgid   left    join     dim_sector   sector  on  alarm.zoneid=sector.zoneid   and     childorg.cloudalarmorgid=sector.cloudalarmorgid   left  join  dim_alarmtype  alarmtype   on  alarm.codeid=alarmtype.codeid   and  alarmtype.cloudalarmcustomid=childorg.cloudalarmcustomid "+
      s" left  join dim_user user2 on  user2.dwuserid=alarm.dwuserid"+
      s"  LEFT JOIN dim_post post2 ON post2.platformpostid = user2.platformpostid"+
      s" left  join  dim_role role2 on  user2.platformuserid=role2.platformuserid"+
      s" left  join  dim_organization userorg on  userorg.platformorgid=user2.platformorgid"+
      s" where  childorg.orglevelcode='04'  ")
      .registerTempTable("alarmreply0")


    hiveContext.sql(s"select   secondlevel.LEVEL,secondlevel.OrgName AS secondorgname  from  dim_organization secondlevel  where  length(secondlevel.`LEVEL`) = 8 ")
      .registerTempTable("secondlevel")

    hiveContext.sql(s"select   thirdlevel.LEVEL,thirdlevel.OrgName AS thirdorgname   from   dim_organization thirdlevel   where  length(thirdlevel.`LEVEL`) = 12 ")
      .registerTempTable("thirdlevel")

    //   (concat(cast(alluser.platformactionid  as varchar),uuid()) )   as rowkey
    hiveContext.sql(s" SELECT   distinct  branchnetname,hostnumber,sectorname,orgtype,customname,secondorgname,thirdorgname,parentorgname," +
      s"   codename  as  alarmtype,cast(alarmnumber as bigint) as alarmnumber,alarmtime, " +
      s"   replyusername,postname,rolename,replyuserorgname,replytime,replycontent,cast(createdate  as  string) as createdate,\'$day\'  as  statisticday   "+
      s"    from  alarmreply0   LEFT JOIN  secondlevel ON   alarmreply0.orglevel8=secondlevel.LEVEL " +
      s"   LEFT JOIN  thirdlevel ON  alarmreply0.orglevel12=thirdlevel.LEVEL  ")
      .registerTempTable("alarmreply2")



    //
    //val  lastdate=currentdate.minusDays(1)
    //  where   alluser.actiondate>=\'$lastmonday\'  and  alluser.actiondate<=\'$lastsunday\'   ")

    //hiveContext.sql("drop table if  exists   default.replyalarm")
    //hiveContext.sql("create table if not exists  default.replyalarm as  select  * from   alarmreply2 ")


    val data=hiveContext.sql("select  * from  alarmreply2 ")
    data.write.mode("append").saveAsTable("default.replyalarm")


    val  dmdf=hiveContext.sql(s"  select  ROW_NUMBER() over(order by customname) AS  rowkey,branchnetname,hostnumber,sectorname,orgtype,customname,secondorgname,thirdorgname,parentorgname,alarmtype,alarmnumber,alarmtime,replyusername,postname,rolename,replyuserorgname,replytime,replycontent,createdate,statisticday  from  default.replyalarm  ")
    //.registerTempTable("replyalarm")


    dmdf.write.mode(SaveMode.Overwrite)
      .options(Map("table" -> "operation.alarmreply",
        "zkUrl" -> "192.168.11.24:60002"))
      .format("org.apache.phoenix.spark").save()




    sc.stop

  }

}
