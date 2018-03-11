package datamarket
/*
每天的布撤防明细
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.phoenix.spark._
import java.time.LocalDate
object deploywithdraw {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dmdeploywithdraw").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //val  currentdate= LocalDate.now()
    val tyyear = args(0)
    val tymonth = args(1)
    val tyday = args(2)
    //System.out.println(s"$tyday")
    val day = tyyear + "-" + tymonth + "-" + tyday
    //println(s"$day")
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    //hive
    hiveContext.sql("use datawarehouse")
    hiveContext.sql(s"  select   dworgid,status,ACTDATE,ACTTIME  from  fact_deploywithdraw      where  ACTDATE=\'$day\'  ")
      .registerTempTable("deploywithdrawtemp")


    hiveContext.sql(s" 	select   childorg.orgname as  branchnetname,childorg.orgtypename   as  orgtype,custom.customname,  org.OrgName AS parentorgname,childorg.orglevel8,  	childorg.orglevel12," +
      s" deploytemp.ACTTIME  as  deploywithdrawtime,deploytemp.ACTDATE  as  deploywithdrawdate,deploytemp.status,deploytemp.ACTDATE as  createdate  " +
      s" from  deploywithdrawtemp  deploytemp   join   dim_organization  childorg  on  deploytemp.dworgid=childorg.dworgid  " +
      s"  join  dim_custom  custom  on  childorg.cloudalarmcustomid=custom.cloudalarmcustomid  LEFT JOIN dim_organization org  on  childorg.cloudalarmparentid=org.cloudalarmorgid  " +
      s" where  childorg.orglevelcode='04'  ")
      .registerTempTable("deploywithdraw0")


    hiveContext.sql(s"select   secondlevel.LEVEL,secondlevel.OrgName AS secondorgname  from  dim_organization secondlevel  where  length(secondlevel.`LEVEL`) = 8 ")
      .registerTempTable("secondlevel")

    hiveContext.sql(s"select   thirdlevel.LEVEL,thirdlevel.OrgName AS thirdorgname   from   dim_organization thirdlevel   where  length(thirdlevel.`LEVEL`) = 12 ")
      .registerTempTable("thirdlevel")

    //   (concat(cast(alluser.platformactionid  as varchar),uuid()) )   as rowkey
    hiveContext.sql(s" SELECT     branchnetname,customname,secondorgname,thirdorgname,parentorgname," +
      s"   orgtype,deploywithdrawtime,cast(deploywithdrawdate as string) as deploywithdrawdate,hour(deploywithdrawtime)  as deploywithdrawhour,status,cast(createdate as string) as  createdate  " +
      s"    from  deploywithdraw0   LEFT JOIN  secondlevel ON   deploywithdraw0.orglevel8=secondlevel.LEVEL " +
      s"   LEFT JOIN  thirdlevel ON  deploywithdraw0.orglevel12=thirdlevel.LEVEL  ")
      .registerTempTable("deploywithdraw2")



    //
    //val  lastdate=currentdate.minusDays(1)
    //  where   alluser.actiondate>=\'$lastmonday\'  and  alluser.actiondate<=\'$lastsunday\'   ")

    //hiveContext.sql("drop table if  exists   default.deploywithdraw")
    //hiveContext.sql("create table if not exists  default.deploywithdraw as  select  * from   deploywithdraw2 ")


    val data=hiveContext.sql("select  * from  deploywithdraw2 ")
    data.write.mode("append").saveAsTable("default.deploywithdraw")


    val dmdf = hiveContext.sql(s" select  ROW_NUMBER() over(order by customname) AS  rowkey,branchnetname,customname,secondorgname,thirdorgname,parentorgname,orgtype,deploywithdrawtime,deploywithdrawdate,deploywithdrawhour,status,createdate   from  default.deploywithdraw  ")
    //.registerTempTable("deploywithdraw")


    dmdf.write.mode(SaveMode.Overwrite)
      .options(Map("table" -> "operation.deploywithdraw",
        "zkUrl" -> "192.168.11.24:60002"))
      .format("org.apache.phoenix.spark").save()


    sc.stop


  }
}
