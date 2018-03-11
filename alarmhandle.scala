package datamarket
/*
±¨¾¯´¦ÖÃ
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.phoenix.spark._
import java.time.LocalDate
object alarmhandle {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dmalarmhandle").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
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
    //???? ????
    //hive
    hiveContext.sql("use datawarehouse")
    // 0011  ????? ???
    // DISTINCT    	concat('0011',alluser.customid,calendaryear,calendarmonth,calendarday,alluser.userid,alluser.userlogid    	)

    hiveContext.sql(s"  select  distinct  dworgid,cloudalarmalarmid,codeid,clientnumber,zoneid,alarmtime,alarmcreatetime,alarmmemo,isfinish,replymemo,passivereplyusername,replytime,cancelalarmtime  from  fact_alarm   where   to_date(alarmcreatetime)=\'$day\'  ")
      .registerTempTable("factalarmtemp")



    hiveContext.sql(s"  select    cloudalarmalarmid,min(replytime) as  replytime   from     factalarmtemp   where passivereplyusername='operateTime'  group by cloudalarmalarmid   ")
      .registerTempTable("onereply0")

    hiveContext.sql(s"  select    (datediff(b.replytime,alarmtime)*24*60)+(cast(hour(b.replytime)*60+minute(b.replytime)+second(b.replytime)/60 as bigint)   -cast(hour(alarmtime)*60+minute(alarmtime)+second(alarmtime)/60 as bigint)) as firsthandleduration,  (datediff(b.replytime,alarmcreatetime)*24*60)+  (cast(hour(b.replytime)*60+minute(b.replytime)+second(b.replytime)/60 as bigint)  -cast(hour(alarmcreatetime)*60+minute(alarmcreatetime)+second(alarmcreatetime)/60 as bigint)) as  firsthandleduration2,  a.cloudalarmalarmid,  b.replytime as  firsthandletime,replymemo as firsthandleresult  from     factalarmtemp  a  join  onereply0 b  on a.cloudalarmalarmid=b.cloudalarmalarmid  where passivereplyusername='operateTime' ")
      .registerTempTable("onereply")


    hiveContext.sql(s"  select   cloudalarmalarmid,min(replytime) as  replytime   from     factalarmtemp   where isfinish=1  and  passivereplyusername='operateTime2'  group by cloudalarmalarmid  ")
      .registerTempTable("secondreply0")

    hiveContext.sql(s"  select    (datediff(d.replytime,alarmtime)*24*60)+(cast(hour(d.replytime)*60+minute(d.replytime)+second(d.replytime)/60 as bigint)   -cast(hour(alarmtime)*60+minute(alarmtime)+second(alarmtime)/60 as bigint)) as secondhandleduration,  (datediff(d.replytime,alarmcreatetime)*24*60)+  (cast(hour(d.replytime)*60+minute(d.replytime)+second(d.replytime)/60 as bigint)  -cast(hour(alarmcreatetime)*60+minute(alarmcreatetime)+second(alarmcreatetime)/60 as bigint)) as  secondhandleduration2,  c.cloudalarmalarmid,  d.replytime as  secondhandletime,replymemo as secondhandleresult  from     factalarmtemp  c  join  secondreply0  d  on  c.cloudalarmalarmid=d.cloudalarmalarmid   where isfinish=1  and  passivereplyusername='operateTime2' ")
      .registerTempTable("secondreply")

    hiveContext.sql(s" select  factalarmtemp.cloudalarmalarmid,response.responsetime,  (datediff(response.responsetime,alarmtime)*24*60)+(cast(hour(response.responsetime)*60+minute(response.responsetime)+second(response.responsetime)/60 as bigint)   -cast(hour(alarmtime)*60+minute(alarmtime)+second(alarmtime)/60 as bigint)) as responseduration,  (datediff(response.responsetime,alarmcreatetime)*24*60)+  (cast(hour(response.responsetime)*60+minute(response.responsetime)+second(response.responsetime)/60 as bigint)  -cast(hour(alarmcreatetime)*60+minute(alarmcreatetime)+second(alarmcreatetime)/60 as bigint)) as  responseduration2   from  factalarmtemp   join (select  cloudalarmalarmid,min(replytime) as responsetime     from  fact_alarm  group  by  cloudalarmalarmid) response  on  factalarmtemp.cloudalarmalarmid=response.cloudalarmalarmid ")
      .registerTempTable("response2")

    hiveContext.sql(s" 	select  distinct   childorg.orgname as  branchnetname,  host.clientnumber   as   hostnumber,sector.name  as  sectorname, " +
      s"  childorg.orgtypename   as  orgtype,custom.customname,  org.OrgName AS parentorgname,childorg.orglevel8,  	childorg.orglevel12,alarmtype.codename,1 as alarmnumber,  	alarm.alarmtime,alarm.cancelalarmtime,alarm.alarmmemo,to_date(alarm.alarmcreatetime) as  createdate,cloudalarmalarmid " +
      s"  from  factalarmtemp  alarm   join   dim_organization  childorg " +
      s"  on    alarm.dworgid=childorg.dworgid    join  dim_custom  custom  on  childorg.cloudalarmcustomid=custom.cloudalarmcustomid " +
      s"  LEFT JOIN   dim_organization  org  on  childorg.cloudalarmparentid=org.cloudalarmorgid   left    join     dim_host  host   on  alarm.clientnumber=host.clientnumber    and      childorg.cloudalarmorgid=host.cloudalarmorgid   left    join     dim_sector   sector  on  alarm.zoneid=sector.zoneid   and     childorg.cloudalarmorgid=sector.cloudalarmorgid   left  join  dim_alarmtype  alarmtype   on  alarm.codeid=alarmtype.codeid   and  alarmtype.cloudalarmcustomid=childorg.cloudalarmcustomid  where  childorg.orglevelcode='04'  ")
      .registerTempTable("alarmhandle0")


    hiveContext.sql(s"select   secondlevel.LEVEL,secondlevel.OrgName AS secondorgname  from  dim_organization secondlevel  where  length(secondlevel.`LEVEL`) = 8 ")
      .registerTempTable("secondlevel")

    hiveContext.sql(s"select   thirdlevel.LEVEL,thirdlevel.OrgName AS thirdorgname   from   dim_organization thirdlevel   where  length(thirdlevel.`LEVEL`) = 12 ")
      .registerTempTable("thirdlevel")

    //   (concat(cast(alluser.platformactionid  as varchar),uuid()) )   as rowkey
    hiveContext.sql(s" SELECT distinct   branchnetname,hostnumber,sectorname,orgtype,customname,secondorgname,thirdorgname,parentorgname," +
      s"   codename  as  alarmtype,cast(alarmnumber as bigint) as alarmnumber,alarmtime,  " +
      s"   firsthandletime,(case  when firsthandleduration<0 then  firsthandleduration2 else firsthandleduration end) as firsthandleduration, "+
      s"   firsthandleresult,secondhandletime,(case  when  secondhandleduration<0  then secondhandleduration2  else  secondhandleduration  end)  as secondhandleduration, "+
      s"   secondhandleresult,cancelalarmtime,alarmmemo as memo," +
      s"   cast(createdate  as  string) as createdate,responsetime,(case when responseduration<0 then responseduration2 else responseduration  end) as responseduration,\'$day\'  as  statisticday  " +
      s"   from  alarmhandle0  left  join  onereply  on  alarmhandle0.cloudalarmalarmid=onereply.cloudalarmalarmid "+
      s"   left  join secondreply  on  alarmhandle0.cloudalarmalarmid=secondreply.cloudalarmalarmid "+
      s"   left  join response2   on   alarmhandle0.cloudalarmalarmid=response2.cloudalarmalarmid "+
      s"   LEFT JOIN  secondlevel ON   alarmhandle0.orglevel8=secondlevel.LEVEL " +
      s"   LEFT JOIN  thirdlevel ON  alarmhandle0.orglevel12=thirdlevel.LEVEL  ")
      .registerTempTable("alarmhandle2")



    //
    //val  lastdate=currentdate.minusDays(1)
    //  where   alluser.actiondate>=\'$lastmonday\'  and  alluser.actiondate<=\'$lastsunday\'   ")

    //hiveContext.sql("drop table if  exists    default.handlealarm")
    //hiveContext.sql("create table if not exists  default.handlealarm as  select  * from   alarmhandle2 ")


    val data=hiveContext.sql("select  * from  alarmhandle2 ")
    data.write.mode("append").saveAsTable("default.handlealarm")


    val  dmdf=hiveContext.sql(s" select   ROW_NUMBER() over(order by customname) AS  rowkey,branchnetname,hostnumber,sectorname,orgtype,customname,secondorgname,thirdorgname,parentorgname,alarmtype,alarmnumber,alarmtime,firsthandletime,firsthandleduration,firsthandleresult,secondhandletime,secondhandleduration,secondhandleresult,cancelalarmtime,memo,createdate,responsetime,responseduration,statisticday  from  default.handlealarm  ")
    //.registerTempTable("alarmhandle")


    dmdf.write.mode(SaveMode.Overwrite)
      .options(Map("table" -> "operation.alarmhandle",
        "zkUrl" -> "192.168.11.24:60002"))
      .format("org.apache.phoenix.spark").save()




    sc.stop

  }

}
