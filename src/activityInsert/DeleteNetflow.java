package activityInsert;

import common.Config;
import tableModel.BaseModel;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 从活动库中删除数据
 * 策略-整表删除,保留3个星期的数据(包括当天时间)
 * Created by yangyun on 2017/6/5.
 */
public class DeleteNetflow {
    /**
     * 获取当天的时间yyyy-MM-dd
     * @return 时间
     */
    public Date getTimeStampOfToday(){
        Date today=new Date(System.currentTimeMillis());
        return today;
    }

    /**
     * 判断是否有数据表需要被删除
     */
    public boolean judgeTable(String tableName){
        Date today=getTimeStampOfToday();
        Date tableNameDate=null;
        try {
            SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
            tableNameDate=sdf.parse(tableName);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        int days= (int) ((today.getTime()-tableNameDate.getTime())/(1000*24*60*60));

        if(days>= Config.KeepAliveTime)
            return true;
        else
            return false;
    }

    /**
     * 数据老化
     */
    public void dataAging(){
        try {
            List<String> tableNames=BaseModel.getTables();//获取所有的数据表
            //进行数据表的格式判断
            for(String tableName:tableNames){
                if(tableName.matches("\\d{4}-\\d{2}-\\d{2}")) {//说明是数据表
                    if (judgeTable(tableName)) {//判断表格是否需要被删除
                        BaseModel.deleteTable(tableName);//删除数据表
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
