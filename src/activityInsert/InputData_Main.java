package activityInsert;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import tableModel.NetflowInfoDAO;

import java.io.*;
import java.net.DatagramSocket;

/**
 * 数据插入模块的main函数
 * Created by yangyun on 16/9/12.
 */
public class InputData_Main {
    private final static int PORT1=10000;
    private final static int PORT2=10002;

    public static final String[] columnFamily=new String[]{
            Bytes.toString(NetflowInfoDAO.SrcIPIndexInfo),
            Bytes.toString(NetflowInfoDAO.DesIPIndexInfo),
            Bytes.toString(NetflowInfoDAO.SrcPortIndexInfo),
            Bytes.toString(NetflowInfoDAO.DesPortIndexInfo)
    };//表格列族

    public static File dataSpeedFile=null;//文件
    public static BufferedWriter out=null;
    public static long count;
    public static int MaxVersion=1;//数据版本数


    public static HTableInterface[] pools=null;//连接池

    private static NetflowInputHBase netflowInputHBase;

    public static void setNetflowInputHBase(String tableName,int ThreadNum,String dateTime){
        netflowInputHBase=new NetflowInputHBase(tableName,ThreadNum,dateTime);
    }

    public static void main(String[] args) throws Exception{
        DatagramSocket serverSocket1 = new DatagramSocket(PORT1);//监听端口10000
        DatagramSocket serverSocket2 = new DatagramSocket(PORT2);//监听端口10002
        int ThreadNum=Integer.valueOf(args[0]);//线程数量先定位6
        pools=new HTableInterface[ThreadNum];//连接池

        while(true){
            String tableName ="20"+ new DateTime().toString("yy-MM-dd HH:mm:ss EE").split("\\s+")[0];
            System.out.format("表名:%s",tableName);

            //文件操作
            dataSpeedFile=new File("/home/hadoop/yangyun/"+tableName);//创建文件
            out = new BufferedWriter(new FileWriter(dataSpeedFile,true));
            //一天的流记录计数
            count=0;

            setNetflowInputHBase(tableName,ThreadNum,tableName);//创建NetflowInputHBase对象
            netflowInputHBase.getNetflowInfoDAO().createNewTable(tableName,columnFamily,MaxVersion);//创建HBase数据表

            for(int i=0;i<pools.length;i++){
                pools[i]=netflowInputHBase.getNetflowInfoDAO().connectTable(tableName);//链接数据表
            }
            netflowInputHBase.NetflowToMemoryThread(serverSocket1,serverSocket2,Integer.valueOf(args[1]));//缓存大小单位是MB
        }
    }
}