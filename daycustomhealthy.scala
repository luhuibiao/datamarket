package datamarket
/*
每日客户健康度
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.phoenix.spark._
import java.time.LocalDate
  object daycustomhealthy {
    def main(args: Array[String]) {
      val sparkConf = new SparkConf().setAppName("customhealthy").setMaster("spark://192.168.11.21:7077")
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

      //public
      hiveContext.sql(s"  select distinct  servicebranchnetid,repairorderno,serviceorderuserid  from  fact_repairmaintenance    where   to_date(ordertime)=\'$day\'  ")
        .registerTempTable("factrepairtemp")

      hiveContext.sql(s" select  servicebranchnetid,count(distinct repairorderno) as  ordersum from  factrepairtemp  group  by  servicebranchnetid ")
          .registerTempTable("dayordersum")


      //客户报单率
      hiveContext.sql(s"  select  repairtemp.servicebranchnetid,count(distinct repairorderno ) as branchnetordersum from   factrepairtemp   repairtemp   join  dim_serviceuser  user2 "+
        s"   on  repairtemp.serviceorderuserid=user2.serviceuserid " +
          s"  where  user2.orgorcompanytype=0 group  by   repairtemp.servicebranchnetid ")
           .registerTempTable("daybranchnetordersum")



      hiveContext.sql(s"  select  org.platformcustomid,org.platformorgid,ordersum,branchnetordersum   " +
        s"  from   dim_organization org  left join  dim_servicebranchnet  servicebranchnet on  org.platformorgid=servicebranchnet.platformorgid " +
        s"  left join     dayordersum a   on   servicebranchnet.servicebranchnetid=a.servicebranchnetid " +
        s"  left join  daybranchnetordersum b " +
        s"   on  a.servicebranchnetid=b.servicebranchnetid  ")
           .registerTempTable("orgrepairorderday0")


     hiveContext.sql(s"  select  platformcustomid,sum(branchnetordersum)/sum(ordersum)  as  customerdeclarationrate  from  orgrepairorderday0  group by platformcustomid  ")
              .registerTempTable("orgrepairorderday")


      //监控中心报单率
      hiveContext.sql(s"  select  repairtemp2.servicebranchnetid,count(distinct repairorderno ) as monitorordersum   from   factrepairtemp  repairtemp2 join  dim_serviceuser  user2 "+
        s"   on  repairtemp2.serviceorderuserid=user2.serviceuserid " +
        s"   left join  dim_servicerole  servicerole  on  user2.serviceroleid=servicerole.serviceroleid" +
        s"   where  servicerole.rolename='监控中心' group  by   repairtemp2.servicebranchnetid ")
        .registerTempTable("daymonitorordersum")

      hiveContext.sql(s"  select  org.platformcustomid,org.platformorgid,ordersum,monitorordersum   " +
        s"  from   dim_organization org  left join  dim_servicebranchnet  servicebranchnet on  org.platformorgid=servicebranchnet.platformorgid " +
        s"  left join     dayordersum a   on   servicebranchnet.servicebranchnetid=a.servicebranchnetid " +
        s"  left join  daymonitorordersum b " +
        s"   on  a.servicebranchnetid=b.servicebranchnetid  ")
        .registerTempTable("monitorrepairorderday0")


      hiveContext.sql(s"  select  platformcustomid,sum(monitorordersum)/sum(ordersum)  as  monitorcenterdeclarationrate  from  monitorrepairorderday0  group by platformcustomid  ")
        .registerTempTable("monitorrepairorderday")

      //网点报修率
      hiveContext.sql(s" select    org.platformcustomid,count(distinct servicebranchnet.servicebranchnetid)  as branchnetnumber     "+
        s"  from   dim_organization  org  left join  dim_servicebranchnet  servicebranchnet on  org.platformorgid=servicebranchnet.platformorgid " +
        s"  group  by  org.platformcustomid")
          .registerTempTable("branchnetrepair")

      hiveContext.sql(s" select    org.platformcustomid,count(distinct repairtemp.servicebranchnetid)  as repairbranchnet     "+
        s"  from   dim_organization  org  left join  dim_servicebranchnet  servicebranchnet on  org.platformorgid=servicebranchnet.platformorgid " +
        s"  left  join   factrepairtemp   repairtemp  on  servicebranchnet.servicebranchnetid=repairtemp.servicebranchnetid   " +
        s"  group  by  org.platformcustomid")
        .registerTempTable("branchnetrepair2")


      hiveContext.sql(s" select  branchnetrepair.platformcustomid,(repairbranchnet/branchnetnumber) as  branchnetrepairrate from  branchnetrepair  left  join branchnetrepair2" +
        s"  on   branchnetrepair.platformcustomid=branchnetrepair2.platformcustomid  ")
           .registerTempTable("branchnetrepairrate")

      //public
      hiveContext.sql(s"  select  distinct  servicebranchnetid,repairorderno,ReceiveTime,RepairTime,RiskMinute,OrderResponseMinute,RepairedResponseMinute  from  fact_repairmaintenance " +
        s"   where   to_date(ordertime)=\'$day\'  ")
        .registerTempTable("repairstatis")


      hiveContext.sql(s" select  servicebranchnetid," +
        s"  sum(RiskMinute) as sumRiskMinute," +
        s"  sum(OrderResponseMinute)   as  sumOrderResponseMinute," +
        s"  sum(RepairedResponseMinute)  as  sumRepairedResponseMinute  from    repairstatis  " +
        s"  group by    servicebranchnetid  ")
        .registerTempTable("repairstatis2")

      //平均故障排除时长
      hiveContext.sql(s"  select   servicebranchnetid,count(distinct  repairorderno)  as countrepairorderno  from   repairstatis " +
        s"  where  RepairTime  is not null " +
        s"  group  by  servicebranchnetid  ")
           .registerTempTable("lengthoftrouble0")



      hiveContext.sql(s" select  repairstatis2.servicebranchnetid,(sumRiskMinute/countrepairorderno)  as avgRiskMinute  from  repairstatis2  join  lengthoftrouble0 " +
        s" on  repairstatis2.servicebranchnetid=lengthoftrouble0.servicebranchnetid  ")
           .registerTempTable("lengthoftrouble2")



      hiveContext.sql(s" select    org.platformcustomid,avg(avgRiskMinute)  as  averagelengthoftrouble     "+
        s"  from   dim_organization  org  left join  dim_servicebranchnet  servicebranchnet on  org.platformorgid=servicebranchnet.platformorgid " +
        s"  left join  lengthoftrouble2  on   servicebranchnet.servicebranchnetid=lengthoftrouble2.servicebranchnetid " +
        s"  group  by  org.platformcustomid")
        .registerTempTable("lengthoftrouble")

      //平均响应时长
      hiveContext.sql(s"  select   servicebranchnetid,count(distinct  repairorderno)  as countrepairorderno  from   repairstatis " +
        s"  where  ReceiveTime  is not null " +
        s"  group  by  servicebranchnetid  ")
        .registerTempTable("responselength0")


      hiveContext.sql(s" select  repairstatis2.servicebranchnetid,(sumOrderResponseMinute/countrepairorderno)  as avgOrderResponseMinute  from  repairstatis2  join  responselength0 " +
        s" on  repairstatis2.servicebranchnetid=responselength0.servicebranchnetid  ")
        .registerTempTable("responselength2")



      hiveContext.sql(s" select    org.platformcustomid,avg(avgOrderResponseMinute)  as  avgresponselength     "+
        s"  from   dim_organization  org  left join  dim_servicebranchnet  servicebranchnet on  org.platformorgid=servicebranchnet.platformorgid " +
        s"  left join  responselength2  on   servicebranchnet.servicebranchnetid=responselength2.servicebranchnetid " +
        s"  group  by  org.platformcustomid")
        .registerTempTable("responselength")


      //故障平均修复时长

      hiveContext.sql(s" select  repairstatis2.servicebranchnetid,(sumRepairedResponseMinute/ordersum)  as avgRepairedResponseMinute from  repairstatis2  join  dayordersum " +
        s" on  repairstatis2.servicebranchnetid=dayordersum.servicebranchnetid  ")
        .registerTempTable("lengthoffaultrepair2")



      hiveContext.sql(s" select    org.platformcustomid,avg(avgRepairedResponseMinute)  as  avglengthoffaultrepair     "+
        s"  from   dim_organization  org  left join  dim_servicebranchnet  servicebranchnet on  org.platformorgid=servicebranchnet.platformorgid " +
        s"  left join  lengthoffaultrepair2  on   servicebranchnet.servicebranchnetid=lengthoffaultrepair2.servicebranchnetid " +
        s"  group  by  org.platformcustomid")
        .registerTempTable("lengthoffaultrepair")


     //巡检达成率
     hiveContext.sql(s"  select   distinct   servicebranchnetid,inspectfrequencybymonth  from  fact_serviceinspect "+
       s" where  inspectfrequencybymonth  is  not   null ")
               .registerTempTable("inspecttemp0")


      hiveContext.sql(s" select    servicebranchnetid,count(distinct  inspectnumber)  as  countinspectnumber   from  fact_serviceinspect"+
        s" where    inspectoriginalstatus=3" +
        s" and   year(inspectstarttime)=\'$tyyear\' and  month(inspectstarttime)=\'$tymonth\'  group  by  servicebranchnetid  ")
               .registerTempTable("inspecttemp1")


      hiveContext.sql(s"  select   inspecttemp0.servicebranchnetid,(countinspectnumber/inspectfrequencybymonth)   as  branchnetinspectionrate   from  inspecttemp0  join  inspecttemp1 " +
        s"  on   inspecttemp0.servicebranchnetid=inspecttemp1.servicebranchnetid  ")
               .registerTempTable("inspecttemp2")


      hiveContext.sql(s" select    org.platformcustomid,avg(branchnetinspectionrate)  as  inspectionrateofreach     "+
        s"  from   dim_organization  org  left join  dim_servicebranchnet  servicebranchnet on  org.platformorgid=servicebranchnet.platformorgid " +
        s"  left join  inspecttemp2  on   servicebranchnet.servicebranchnetid=inspecttemp2.servicebranchnetid " +
        s"  group  by  org.platformcustomid")
        .registerTempTable("inspectionrate")



      //上面是服务指标
      //下面是报警指标
      hiveContext.sql("select  distinct  cloudalarmorgid,cloudalarmalarmid   from    fact_alarm  where  to_date(alarmtime)=\'$day\' " )
              .registerTempTable("alarmtemp")

      hiveContext.sql("select  cloudalarmorgid,count(distinct cloudalarmalarmid) as countalarm from alarmtemp " +
        "  group  by  cloudalarmorgid ")
          .registerTempTable("alarmcount")


      hiveContext.sql("select  a.cloudalarmorgid,sum(b.firsthandleduration) as  firsthandleduration  from " +
        "  alarmtemp   a  join   fact_alarmhandle    b  on  a.cloudalarmalarmid=b.cloudalarmalarmid  where  b.firsthandletime is not null " +
        "  group  by   a.cloudalarmorgid ")
                .registerTempTable("firsthandletemp0")

      hiveContext.sql("select    a.cloudalarmorgid,(b.firsthandleduration/a.countalarm) as  avgfirsthandle    from alarmcount  a  " +
        "   left join  firsthandletemp0  b    on  a.cloudalarmorgid=b.cloudalarmorgid  ")
                .registerTempTable("firsthandletemp")
      //报警一次处置平均时间
      hiveContext.sql(s" select    org.platformcustomid,avg(avgfirsthandle)  as  alarmfirsthandleavgtime     "+
        s"  from   dim_organization  org  left join  firsthandletemp   on  org.cloudalarmorgid=firsthandletemp.cloudalarmorgid " +
        s"  group  by  org.platformcustomid")
                 .registerTempTable("firsthandleavg")


      //
      hiveContext.sql("select  a.cloudalarmorgid,sum(b.secondhandleduration) as  secondhandleduration  from " +
        "  alarmtemp   a  join   fact_alarmhandle    b  on  a.cloudalarmalarmid=b.cloudalarmalarmid  where  b.secondhandletime is not null " +
        "  group  by   a.cloudalarmorgid ")
        .registerTempTable("secondhandletemp0")

      hiveContext.sql("select    a.cloudalarmorgid,(b.secondhandleduration/a.countalarm) as  avgsecondhandle    from alarmcount  a  " +
        "   left join  secondhandletemp0  b    on  a.cloudalarmorgid=b.cloudalarmorgid  ")
        .registerTempTable("secondhandletemp")
      //报警二次处置平均时间
      hiveContext.sql(s" select    org.platformcustomid,avg(avgsecondhandle)  as  alarmsecondhandleavgtime     "+
        s"  from   dim_organization  org  left join  secondhandletemp   on  org.cloudalarmorgid=secondhandletemp.cloudalarmorgid " +
        s"  group  by  org.platformcustomid")
        .registerTempTable("secondhandleavg")

      //
      hiveContext.sql("select  a.cloudalarmorgid,count(distinct  a.cloudalarmalarmid) as  counthandlealarm  from " +
        "  alarmtemp   a  join   fact_alarmhandle    b  on  a.cloudalarmalarmid=b.cloudalarmalarmid  where  (b.firsthandletime is not null)  or  (b.secondhandletime is not null) " +
        "  group  by   a.cloudalarmorgid ")
        .registerTempTable("counthandlealarmtemp")

      hiveContext.sql("select  a.cloudalarmorgid,(counthandlealarm/countalarm) as  avgnumberhandlealarm  from " +
        "  alarmcount  a   join   counthandlealarmtemp   b  on  a.cloudalarmorgid=b.cloudalarmorgid  " +
        "  ")
        .registerTempTable("avghandlealarmnumber")


      //报警处置率
      hiveContext.sql(s" select    org.platformcustomid,avg(avgnumberhandlealarm)  as  alarmhandlerate     "+
        s"  from   dim_organization  org  left join  avghandlealarmnumber   on  org.cloudalarmorgid=avghandlealarmnumber.cloudalarmorgid " +
        s"  group  by  org.platformcustomid")
        .registerTempTable("ratealarmhandle")



      //
      hiveContext.sql("select  a.cloudalarmorgid,count(distinct  a.cloudalarmalarmid) as  countresponsealarm  from " +
        "  alarmtemp   a  join   fact_alarmhandle    b  on  a.cloudalarmalarmid=b.cloudalarmalarmid  where   b.responsetime is not null " +
        "  group  by   a.cloudalarmorgid ")
        .registerTempTable("countresponsealarmtemp")

      hiveContext.sql("select  a.cloudalarmorgid,(countresponsealarm/countalarm) as  avgnumberresponsealarm  from " +
        "  alarmcount  a   join   countresponsealarmtemp   b  on  a.cloudalarmorgid=b.cloudalarmorgid  " +
        "   ")
        .registerTempTable("avgresponsealarmnumber")

      //报警响应率
      hiveContext.sql(s" select    org.platformcustomid,avg(avgnumberresponsealarm)  as  alarmresponserate     "+
        s"  from   dim_organization  org  left join  avgresponsealarmnumber   on  org.cloudalarmorgid=avgresponsealarmnumber.cloudalarmorgid " +
        s"  group  by  org.platformcustomid")
        .registerTempTable("ratealarmresponse")

      //
      hiveContext.sql("select  a.cloudalarmorgid,sum(b.responseduration) as  responseduration  from " +
        "  alarmtemp   a  join   fact_alarmhandle    b  on  a.cloudalarmalarmid=b.cloudalarmalarmid  where  b.responsetime is not null " +
        "  group  by   a.cloudalarmorgid ")
        .registerTempTable("responsetemp0")




      hiveContext.sql("select  a.cloudalarmorgid,(responseduration/countresponsealarm) as  alarmresponseavg  from " +
        "  countresponsealarmtemp  a   join   responsetemp0   b  on  a.cloudalarmorgid=b.cloudalarmorgid  " +
        "   ")
        .registerTempTable("avgalarmresponse")



      //报警平均响应时间
      hiveContext.sql(s" select    org.platformcustomid,avg(alarmresponseavg)  as  alarmresponseavgtime     "+
        s"  from   dim_organization  org  left join  avgalarmresponse   on  org.cloudalarmorgid=avgalarmresponse.cloudalarmorgid " +
        s"  group  by  org.platformcustomid")
        .registerTempTable("timealarmresponseavg")


      //
      hiveContext.sql("select     status,cloudalarmcustomid,cloudalarmorgid,timenode   from    fact_deploystatus   where  timenode=\'$day\' " )
        .registerTempTable("deploystatustemp")

      hiveContext.sql("select  cloudalarmcustomid,count(distinct cloudalarmorgid) as statusorg from deploystatustemp " +
        "    group  by  cloudalarmcustomid ")
        .registerTempTable("orgstatus")


      hiveContext.sql("select  cloudalarmcustomid,count(distinct cloudalarmorgid) as statusdeploy from deploystatustemp " +
        "   where status='布防'  group  by  cloudalarmcustomid ")
        .registerTempTable("deploystatus")

      //布防率
      hiveContext.sql(s" select    org.platformcustomid,(statusdeploy/statusorg)  as  deployrate     "+
        s"  from   dim_organization  org  left join  orgstatus   on  org.cloudalarmcustomid=orgstatus.cloudalarmcustomid " +
        s"   left join  deploystatus   on  orgstatus.cloudalarmcustomid=deploystatus.cloudalarmcustomid " +
        s"   ")
        .registerTempTable("ratedeploy")

      //未布防率
      hiveContext.sql(s"select  platformcustomid,(1-deployrate)  as  undeployrate  from    ratedeploy ")
        .registerTempTable("unratedeploy")


      //
      hiveContext.sql("select  cloudalarmorgid,count(distinct cloudalarmalarmid) as  undeployalarmnumber from   fact_alarm   where    replymemo='人为1：未及时撤防'  " +
        "    group  by  cloudalarmorgid ")
        .registerTempTable("undeployalarm")

      //未撤防进入网点报警数
      hiveContext.sql(s" select    org.platformcustomid,sum(undeployalarmnumber)  as  undeploytonetalarmnumber     "+
        s"  from    dim_organization  org  left join  undeployalarm   on  org.cloudalarmorgid=undeployalarm.cloudalarmorgid " +
        s"  group   by  org.platformcustomid")
        .registerTempTable("undeploytonetalarm")

      //上面是报警
      //下面是用户相关的

      //累计在职用户数
      hiveContext.sql("select   platformcustomid,count(*)   as cumulativeofonpostusers    from    dim_user   where status=0  "+
        s"  and  to_date(createtime)<=\'$day\'   group  by  platformcustomid	  " )
        .registerTempTable("usercount")
      //新增用户数
      hiveContext.sql("select   platformcustomid,count(*) as numberofnewusers    from    dim_user   where status=0  "+
        s"  and  to_date(createtime)=\'$day\'   group  by  platformcustomid	  " )
        .registerTempTable("newusers")

      //在职用户数
      hiveContext.sql("select   platformcustomid,count(*) as  numberofleaveusers    from    dim_user   where status=1  "+
        s"  and  to_date(statusdate)=\'$day\'   group  by  platformcustomid	  " )
        .registerTempTable("leaveusers")


      //
      hiveContext.sql("select  b.platformcustomid,count(distinct  b.platformuserid) as centernumber   "+
        s" from  fact_useronlinerate a join   dim_user   b   on  a.userid=b.platformuserid   "+
        s" join  dim_role  c on  b.platformuserid=c.platformuserid " +
        s"  where   timenode=\'$day\'  and  rolename='管理人员'  group  by  b.platformcustomid " )
               .registerTempTable("numbercenter")



      hiveContext.sql("select  b.platformcustomid,count(distinct  b.platformuserid) as centeronlinenumber  from  fact_useronlinerate a join   dim_user   b   on  a.userid=b.platformuserid   "+
        s" join  dim_role  c on  b.platformuserid=c.platformuserid " +
        s"  where isonline=1 and     timenode=\'$day\' and rolename='管理人员'  group  by  b.platformcustomid " )
               .registerTempTable("onlinenumbercenter")


      //中心上线率
      hiveContext.sql(s" select    numbercenter.platformcustomid,(centeronlinenumber/centernumber)  as  centeronlinerate     "+
        s"  from    numbercenter   left join  onlinenumbercenter   on  numbercenter.platformcustomid=onlinenumbercenter.platformcustomid ")
        .registerTempTable("ratecenteronline")

      //
      hiveContext.sql("select  b.platformcustomid,count(distinct  b.platformuserid) as commonnumber   "+
        s"   from  fact_useronlinerate a join   dim_user   b   on  a.userid=b.platformuserid   "+
        s"   join  dim_role  c on  b.platformuserid=c.platformuserid " +
        s"    where     timenode=\'$day\'  and  rolename='普通员工'  group  by  b.platformcustomid " )
             .registerTempTable("numbercommon")



      hiveContext.sql("select  b.platformcustomid,count(distinct  b.platformuserid) as commonlinenumber  from  fact_useronlinerate a join   dim_user   b   on  a.userid=b.platformuserid   "+
        s" join  dim_role  c on  b.platformuserid=c.platformuserid " +
        s"  where isonline=1   and  timenode=\'$day\'  and   rolename='普通员工'  group  by  b.platformcustomid " )
        .registerTempTable("numbercommonline")


      //网点上线率
      hiveContext.sql(s" select    numbercommon.platformcustomid,(commonlinenumber/commonnumber)  as  branchnetonlinerate     "+
        s"  from    numbercommon   left join  numbercommonline   on  numbercommon.platformcustomid=numbercommonline.platformcustomid ")
        .registerTempTable("ratebranchnetonline")


       //
      hiveContext.sql("select  b.platformcustomid,count(distinct  b.platformuserid) as cumulativeonline  from  fact_useronlinerate a join   dim_user   b   on  a.userid=b.platformuserid   "+
        s" where  isonline=1   and  timenode=\'$day\'     group  by  b.platformcustomid " )
        .registerTempTable("onlinecumulative")


      //累积上线率
      hiveContext.sql(s" select    usercount.platformcustomid,(cumulativeonline/cumulativeofonpostusers)  as  cumulativeonlinerate  "+
        s"  from    usercount   left join  onlinecumulative   on  usercount.platformcustomid=onlinecumulative.platformcustomid ")
        .registerTempTable("ratecumulativeonline")




      //汇总
      hiveContext.sql(s"select  distinct  custom.customname,cumulativeofonpostusers,numberofnewusers,numberofleaveusers,centeronlinerate,branchnetonlinerate,cumulativeonlinerate,alarmfirsthandleavgtime,alarmsecondhandleavgtime,alarmhandlerate,"+
        s"   alarmresponserate,year(\'$day\') as  statisticyear,\'$day\'  as  startdate,0  as statisticperiod from   dim_custom custom "+
        s"  left  join  usercount       on     usercount.platformcustomid=custom.platformcustomid"+
        s"  left  join  newusers         on      newusers.platformcustomid=custom.platformcustomid"+
        s"  left  join  leaveusers      on     leaveusers.platformcustomid=custom.platformcustomid"+
        s"  left  join  ratecenteronline     on     ratecenteronline.platformcustomid=custom.platformcustomid "+
        s"  left  join  ratebranchnetonline     on     ratebranchnetonline.platformcustomid=custom.platformcustomid "+
        s"  left  join  ratecumulativeonline     on     ratecumulativeonline.platformcustomid=custom.platformcustomid "+
        s"  left  join  firsthandleavg         on     firsthandleavg.platformcustomid=custom.platformcustomid"+
        s"  left  join  secondhandleavg        on     secondhandleavg.platformcustomid=custom.platformcustomid"+
        s"  left  join  ratealarmhandle        on     ratealarmhandle.platformcustomid=custom.platformcustomid"+
        s"  left  join  ratealarmresponse      on     ratealarmresponse.platformcustomid=custom.platformcustomid ")
          .registerTempTable("daycustomhealthytemp0")

       hiveContext.sql(s"select  distinct  custom.customname,alarmresponseavgtime,deployrate,undeployrate,undeploytonetalarmnumber,customerdeclarationrate,"+
        s"   monitorcenterdeclarationrate,branchnetrepairrate,averagelengthoftrouble,inspectionrateofreach,"+
        s"   avgresponselength,avglengthoffaultrepair,\'$day\'  as  startdate from   dim_custom custom "+
        s"  left  join  timealarmresponseavg   on     timealarmresponseavg.platformcustomid=custom.platformcustomid"+
        s"  left  join  ratedeploy             on     ratedeploy.platformcustomid=custom.platformcustomid"+
        s"  left  join  unratedeploy           on     unratedeploy.platformcustomid=custom.platformcustomid"+
        s"  left  join  undeploytonetalarm     on     undeploytonetalarm.platformcustomid=custom.platformcustomid"+
        s"  left  join  orgrepairorderday      on     orgrepairorderday.platformcustomid=custom.platformcustomid"+
        s"  left  join  monitorrepairorderday  on     monitorrepairorderday.platformcustomid=custom.platformcustomid"+
        s"  left  join  branchnetrepairrate    on     branchnetrepairrate.platformcustomid=custom.platformcustomid"+
        s"  left  join  lengthoftrouble        on     lengthoftrouble.platformcustomid=custom.platformcustomid"+
        s"  left  join  responselength         on     responselength.platformcustomid=custom.platformcustomid"+
        s"  left  join  lengthoffaultrepair    on     lengthoffaultrepair.platformcustomid=custom.platformcustomid"+
        s"  left  join  inspectionrate         on     inspectionrate.platformcustomid=custom.platformcustomid ")
          .registerTempTable("daycustomhealthytemp1")




      //val  lastdate=currentdate.minusDays(1)
      //  where   alluser.actiondate>=\'$lastmonday\'  and  alluser.actiondate<=\'$lastsunday\'   ")

      //hiveContext.sql("drop table if  exists   default.daycustomhealthy")
      //hiveContext.sql("create table if not exists  default.daycustomhealthy0 as  select  * from   daycustomhealthytemp0 ")
      val  data0=hiveContext.sql(" select  * from   daycustomhealthytemp0 ")
      data0.write.mode("append").saveAsTable("default.daycustomhealthy0")


      //hiveContext.sql("drop table if  exists   default.daycustomhealthy1")
      //hiveContext.sql("create table if not exists  default.daycustomhealthy1 as  select  * from   daycustomhealthytemp1 ")
      val  data1=hiveContext.sql(" select  * from   daycustomhealthytemp1 ")
      data1.write.mode("append").saveAsTable("default.daycustomhealthy1")


      val dmdf=hiveContext.sql("select   ROW_NUMBER() over(order by temp0.customname) AS  rowkey,temp0.customname,cumulativeofonpostusers,numberofnewusers,numberofleaveusers,centeronlinerate,branchnetonlinerate,"+
        s"   cumulativeonlinerate,alarmfirsthandleavgtime,alarmsecondhandleavgtime,"+
        s"   alarmhandlerate,alarmresponserate,alarmresponseavgtime,deployrate,undeployrate,undeploytonetalarmnumber, "+
        s"   customerdeclarationrate,monitorcenterdeclarationrate,branchnetrepairrate,"+
        s"   averagelengthoftrouble,inspectionrateofreach,avgresponselength,avglengthoffaultrepair,"+
        s"   statisticyear,temp0.startdate,statisticperiod from  default.daycustomhealthy0  temp0  join  default.daycustomhealthy1 temp1 on temp0.customname=temp1.customname  and temp0.startdate=temp1.startdate  ")


      /*
      val  dmdf=hiveContext.sql(s"  select  ROW_NUMBER() over(order by customname) AS  rowkey,"+
        s"   customname,cumulativeofonpostusers,numberofnewusers,numberofleaveusers,centeronlinerate,branchnetonlinerate,"+
        s"   cumulativeonlinerate,alarmfirsthandleavgtime,alarmsecondhandleavgtime,"+
        s"   alarmhandlerate,alarmresponserate,alarmresponseavgtime,deployrate,undeployrate,undeploytonetalarmnumber, "+
        s"   customerdeclarationrate,monitorcenterdeclarationrate,branchnetrepairrate,"+
        s"   averagelengthoftrouble,inspectionrateofreach,avgresponselength,avglengthoffaultrepair,"+
        s"   statisticyear,startdate,statisticperiod"+
        s"   from  default.daycustomhealthy  ")
       */


      dmdf.write.mode(SaveMode.Overwrite)
        .options(Map("table" -> "public.daycustomhealthy",
          "zkUrl" -> "192.168.11.24:60002"))
        .format("org.apache.phoenix.spark").save()




      sc.stop

    }

  }

