package tableModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * 数据表操作类的基类
 * Created by yangyun on 16/9/11.
 */
public class BaseModel {
    private static Configuration configuration=null;
    private static HBaseAdmin hBaseAdmin=null;
    private static HTablePool hTablePool=null;

    static {
        configuration= HBaseConfiguration.create();
        hTablePool=new HTablePool();
        try {
            hBaseAdmin = new HBaseAdmin(configuration);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 创建新HBase表格
     * 目前的表结构
     * 四种索引结构--四个列族
     *1.源地址索引
     *2.宿地址索引
     *3.源端口索引
     *4.宿端口索引
     * @param tableName 表格名称
     * @param columnFamilys 列族名族
     * @param maxVersion 数据的保留版本数
     * @throws IOException
     */
    public static void createNewTable(String tableName,String[] columnFamilys,int maxVersion) throws IOException{
        if(hBaseAdmin.tableExists(tableName))
            System.out.println("Table : "+tableName+" is exist");
        else {
            HTableDescriptor hTableDescriptor=new HTableDescriptor(tableName);
            for(String columnFamily:columnFamilys){
                HColumnDescriptor hColumnDescriptor=new HColumnDescriptor(columnFamily);
                hColumnDescriptor.setMaxVersions(maxVersion);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            hBaseAdmin.createTable(hTableDescriptor);//根据列族创建表格
            System.out.println("Table : "+tableName+" is created successfully");
        }
    }

    /**
     * 删除表格
     * @param tableName 表格名称
     * @throws IOException
     */
    public static void deleteTable(String tableName)throws IOException{
        if(hBaseAdmin.isTableAvailable(Bytes.toBytes(tableName))){
            hBaseAdmin.disableTable(Bytes.toBytes(tableName));
            hBaseAdmin.deleteTable(Bytes.toBytes(tableName));
            System.out.println(tableName+" delete successfully!");
        }else
            System.out.println("Table : "+tableName+" is not exist");
    }

    /**
     * 链接数据表
     * @param tableName 数据表名
     * @return 表格链接
     */
    public HTableInterface connectTable(String tableName){
        HTableInterface hTableInterface=hTablePool.getTable(tableName);
        return hTableInterface;
    }

    /**
     * 关闭数据表
     */
    public void closeConnect(HTableInterface hTableInterface) throws IOException{
        hTableInterface.close();
    }

    /**
     * 获取活动库所有的表名
     * @return List<String>
     */
    public static List<String> getTables()throws IOException{
        List<String> tables=new ArrayList<String>();
        for(TableName tableName:hBaseAdmin.listTableNames()){
            tables.add(tableName.toString());
        }
        return tables;
    }
}
