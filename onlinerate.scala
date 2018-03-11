package datamarket
/*
上线率
输入日期
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.phoenix.spark._
import java.time.LocalDate
object onlinerate {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dmonlinerate").setMaster("spark://192.168.11.21:7077")
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
    //val df =sqlContext.sql(s"SELECT DISTINCT   concat(alluser.customid,year(current_timestamp()),month(current_timestamp())-1,alluser.userid,alluser.userlogid) as rowkey, 	alluser.userid,    	alluser.username,    	alluser.postname,    	alluser.RoleName,    	alluser.customname,    	secondlevel.OrgName AS secondorgname,    	thirdlevel.OrgName AS thirdorgname,    	alluser.parentorgname,    	alluser.orgname,      calendar.calendarday as statisticday,    	(case  alluser.actiondate  when  calendar.calendarday then 1 else  0 end) as isonline,    	alluser.lasttime,    	alluser.isleave,    	alluser.createtime,    	alluser.canceltime    FROM    dict_calendar  calendar      left  join      	(    		SELECT    			userinfo.ID AS userid,    			userinfo.Name AS username,    			post.PostName AS postname,    			role.RoleName AS RoleName,   account.ID AS customid,   			account.AccountName AS customname,userlog.ID  as  userlogid,    			org.OrgName AS parentorgname,    			childorg.OrgName AS orgname,    			childorg.Level AS orglevel,    			to_date(userlog.ACTDATE)  as actiondate,    			userinfo.Updatetime AS lasttime,    			(    				CASE userinfo.Status   				WHEN 1 THEN    					'0'    				ELSE    					'1'    				END    			) AS isleave,    			userinfo.CreateTime AS createtime,    			userinfo.StatusDate AS canceltime    		FROM    			pl_userinfo userinfo    		LEFT JOIN pl_organization childorg ON userinfo.OrgID = childorg.ID    		LEFT JOIN pl_organization org ON org.ID = childorg.ParentID    		LEFT JOIN pl_post post ON post.ID = userinfo.PostID    		LEFT JOIN pl_user_role_rel rolerel ON rolerel.UserID = userinfo.ID    		LEFT JOIN pl_roles role ON rolerel.RoleID = role.ID    		LEFT JOIN pl_openaccount account ON userinfo.OpenAccountID = account.ID    		AND childorg.OpenAccountID = account.ID    		LEFT JOIN t_useractionlog userlog ON userlog.USERID = userinfo.ID    		AND userlog.ORGANIZATIONID = childorg.ID    		AND userlog.CUSTOMID = account.ID    	) alluser    on  calendar.calendarday=alluser.actiondate    LEFT JOIN pl_organization secondlevel ON length(secondlevel.Level) = 8    AND secondlevel.Level LIKE CONCAT(alluser.orglevel, '%')    LEFT JOIN pl_organization thirdlevel ON length(thirdlevel.Level) = 12    AND thirdlevel.Level LIKE CONCAT(alluser.orglevel, '%')  where   alluser.actiondate>=\'$lastmonday\'  and  alluser.actiondate<=\'$lastsunday\'   ")
    //hive
    hiveContext.sql("use datawarehouse")
    // 0011  ????? ???
    // DISTINCT    	concat('0011',alluser.customid,calendaryear,calendarmonth,calendarday,alluser.userid,alluser.userlogid    	)
    //val  df=hiveContext.sql(s" SELECT     Row_Number() OVER(order  by   alluser.userid) AS rowkey,    	alluser.userid,    	alluser.username,    	alluser.postname,    	alluser.RoleName,    	alluser.customname,    	secondlevel.OrgName AS secondorgname,    	thirdlevel.OrgName AS thirdorgname,    	alluser.parentorgname,    	alluser.orgname,    	calendar.daytime  AS statisticday,    	(    		CASE alluser.actiondate    		WHEN calendar.daytime  THEN    			1    		ELSE    			0    		END    	) AS isonline,    	alluser.lasttime,    	alluser.isleave,    	alluser.createtime,    	alluser.canceltime    FROM    	dict_calendar calendar    LEFT JOIN (    	SELECT    		userinfo.platformuserid AS userid,    		userinfo.NAME AS username,    		post.PostName AS postname,    		`role`.RoleName AS RoleName,    		account.platformcustomid AS customid,    		account.customname,    		userlog.actionid AS userlogid,    		org.OrgName AS parentorgname,    		childorg.OrgName AS orgname,    		childorg.`LEVEL` AS orglevel,    		to_date (userlog.actiontime) AS actiondate,    		userinfo.Updatetime AS lasttime,    		(    			CASE userinfo. STATUS    			WHEN 1 THEN    				'0'    			ELSE    				'1'    			END    		) AS isleave,    		userinfo.CreateTime AS createtime,    		userinfo.StatusDate AS canceltime    	FROM    		dim_user userinfo    	LEFT JOIN dim_organization childorg ON userinfo.platformorgid = childorg.platformorgid    	LEFT JOIN dim_organization org ON org.platformorgid = childorg.ParentID    	LEFT JOIN dim_post post ON post.platformpostid = userinfo.platformpostid    	LEFT JOIN dim_role `role` ON userinfo.platformuserid = `role`.platformuserid    	LEFT JOIN dim_custom account ON userinfo.platformcustomid = account.platformcustomid    	AND childorg.platformcustomid = account.platformcustomid    	LEFT JOIN  fact_useraction userlog ON userlog.platformuserid = userinfo.platformuserid    	AND userlog.platformorgid = childorg.platformorgid    	AND userlog.platformcustomid = account.platformcustomid    ) alluser ON calendar.daytime = alluser.actiondate    LEFT JOIN dim_organization secondlevel ON length(secondlevel.`LEVEL`) = 8    AND secondlevel. LEVEL LIKE CONCAT(alluser.orglevel, '%')    LEFT JOIN dim_organization thirdlevel ON length(thirdlevel.`LEVEL`) = 12    AND thirdlevel. LEVEL LIKE CONCAT(alluser.orglevel, '%')    where   alluser.actiondate=\'$day\'  ")
    hiveContext.sql(s"  select   platformuserid,to_date(max(actiontime)) AS actiondate,max(actiontime) as   lasttime  from  fact_useraction   where   to_date(actiontime)=\'$day\' " +
      s"    group  by   platformuserid  ")
      .registerTempTable("factuseractiontemp")


    hiveContext.sql(s" 	SELECT  distinct  		userinfo.platformuserid AS userid,    		userinfo. NAME AS username,    		post.PostName AS postname,    		`role`.RoleName AS RoleName,    		account.platformcustomid AS customid,    		account.customname,    		org.OrgName AS parentorgname,childorg.OrgName AS orgname,childorg.orglevel8,childorg.orglevel12," +
      s" userlog.actiondate,  userlog.lasttime,    		(    			CASE userinfo. STATUS    			WHEN 1 THEN    				'0'    			ELSE    				'1'    			END    		) AS isleave,    		userinfo.CreateTime AS createtime,    		userinfo.StatusDate AS canceltime    	" +
      s" FROM    		dim_user userinfo    	LEFT JOIN dim_organization childorg ON userinfo.platformorgid = childorg.platformorgid    	LEFT JOIN dim_organization org ON org.platformorgid = childorg.platformparentid   " +
      s" 	LEFT JOIN dim_post post ON post.platformpostid = userinfo.platformpostid    	LEFT JOIN dim_role `role` ON userinfo.platformuserid = `role`.platformuserid    " +
      s"	LEFT JOIN dim_custom account ON userinfo.platformcustomid = account.platformcustomid    	AND childorg.platformcustomid = account.platformcustomid    " +
      s"	LEFT JOIN factuseractiontemp userlog ON userlog.platformuserid = userinfo.platformuserid       ")
      .registerTempTable("alluser")


    hiveContext.sql(s"select   secondlevel.LEVEL,secondlevel.OrgName AS secondorgname  from  dim_organization secondlevel  where  length(secondlevel.`LEVEL`) = 8 ")
      .registerTempTable("secondlevel")

    hiveContext.sql(s"select   thirdlevel.LEVEL,thirdlevel.OrgName AS thirdorgname   from   dim_organization thirdlevel   where  length(thirdlevel.`LEVEL`) = 12 ")
      .registerTempTable("thirdlevel")

    //   (concat(cast(alluser.platformactionid  as varchar),uuid()) )   as rowkey
    hiveContext.sql(s" SELECT   cast(alluser.userid as bigint), alluser.username,    	alluser.postname,    	alluser.RoleName,    	alluser.customname,    	secondlevel.secondorgname,    	thirdlevel.thirdorgname,    	alluser.parentorgname,    	alluser.orgname,    	calendar.daytime AS statisticday,    cast(	(    		CASE alluser.actiondate    		WHEN calendar.daytime THEN    			1    		ELSE    			0    		END    	)  as  bigint) AS isonline  ,    	alluser.lasttime,    	cast(alluser.isleave  as  bigint),    	alluser.createtime,    	alluser.canceltime  " +
      s"  FROM    	dict_calendar calendar,alluser   " +
      s"   LEFT JOIN  secondlevel ON   alluser.orglevel8=secondlevel.LEVEL " +
      s"   LEFT JOIN  thirdlevel ON  alluser.orglevel12=thirdlevel.LEVEL  " +
      s"   where  alluser.userid is not  null " +
      s"   and  calendar.daytime = \'$day\'   ")
      .registerTempTable("alluser2")

    //write into  datawarehouse.fact_useronlinerate
    val data0=hiveContext.sql("select  distinct    '0011'  as   sourceflag,userid as  platformuserid,isonline,lasttime,STATISTICDAY as timenode from  alluser2 ")
    data0.write.mode("append").saveAsTable("datawarehouse.fact_useronlinerate")

    //
    //val  lastdate=currentdate.minusDays(1)
    //  where   alluser.actiondate>=\'$lastmonday\'  and  alluser.actiondate<=\'$lastsunday\'   ")


    //hiveContext.sql("drop table if  exists   default.onlinerate")
    //hiveContext.sql("create table if not exists  default.onlinerate as  select  * from   alluser2 ")



    val data=hiveContext.sql("select  * from  alluser2 ")
    data.write.mode("append").saveAsTable("default.onlinerate")


    val  dmdf=hiveContext.sql(s" select  ROW_NUMBER() over(order by customname) AS  rowkey,userid,username,postname,RoleName,customname,secondorgname,thirdorgname,parentorgname,orgname,statisticday,isonline,lasttime,isleave,createtime,canceltime  from  default.onlinerate  ")
    //.registerTempTable("dmonlinerate")




    dmdf.write.mode(SaveMode.Overwrite)
      .options(Map("table" -> "operation.onlinerate",
        "zkUrl" -> "192.168.11.24:60002"))
      .format("org.apache.phoenix.spark").save()




    sc.stop

  }
}
