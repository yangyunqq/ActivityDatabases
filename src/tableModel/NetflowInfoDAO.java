package tableModel;

import activityQuery.CommonTools;
import com.kenai.jaffl.struct.Struct;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * netflow数据表相关操作--工具类
 * Created by yangyun on 16/9/11.
 */
public class NetflowInfoDAO extends BaseModel{
    private byte[] tableName;

    public static final byte[] SrcIPIndexInfo=Bytes.toBytes("SrcIPIndex");//源地址索引对应的数据
    public static final byte[] DesIPIndexInfo=Bytes.toBytes("DesIPIndex");//宿地址索引对应的数据
    public static final byte[] SrcPortIndexInfo=Bytes.toBytes("SrcPortIndex");//源端口索引对应的数据
    public static final byte[] DesPortIndexInfo=Bytes.toBytes("DesPortIndex");//宿端口索引对应的数据

    public static final byte[] info=Bytes.toBytes("netflowInfo");//每个列族的数据


    static Configuration configuration= HBaseConfiguration.create();
    static Connection connection=null;
    static {
        //configuration.set("hbase.client.pause", "50");
        //configuration.set("hbase.client.retries.number", "3");
        //configuration.set("hbase.rpc.timeout", "2000");
        //configuration.set("hbase.client.operation.timeout", "3000");
        configuration.set("hbase.client.scanner.timeout.period", "120000");//scan的超时时间设置120秒
    }

    public NetflowInfoDAO(byte[] tableName){
        this.tableName=tableName;
    }

    /**
     * 设置行键
     * 前缀+时间+随机数(不要了)
     * @param
     * @param dateTime 时间
     * @return
     */
    private byte[] getRowKey(String prefix,String data, DateTime dateTime){
        String rowKey=prefix+"|"+data+"|"+dateTime.getMillis()*-1;
        return Bytes.toBytes(rowKey);
    }

    /**
     * 向数据库中写入数据
     */
    private Put mPut(String prefix,String data,DateTime dateTime,String flow,byte[] columnFamily,byte[] column){
        Put put=new Put(getRowKey(prefix,data,dateTime));
        put.setWriteToWAL(false);//关闭WAL
        put.addColumn(columnFamily,column,Bytes.toBytes(flow));
        return put;
    }

    /**
     * 按照源IP扫描info列族中的信息,扫描一个region
     * @param data-------只支持IP地址扫描
     * @param seg 为地址段收尾IP地址之间的差
     * @param family
     * @return
     */
    private Scan mScan(String prefix,String data,long seg,byte[] family){

        String startKey=prefix+"|"+CommonTools.getIPNum(data)+"|-0000000000000";
        String endKey= prefix+"|"+(CommonTools.getIPNum(data)+seg)+"|-0000000000000";

        byte[] startRow = Bytes.toBytes(startKey);
        byte[] stopRow = Bytes.toBytes(endKey);

        Scan scan=new Scan(startRow,stopRow);
        scan.addFamily(family);
        return scan;
    }

    /**
     * 插入一条信息
     * 注意:这里的表格操作函数并没有对表格进行链接
     * 因为每次put都链接十分的影响效率,可以在主程序中链接一次就可以了
     */
    public Put putOneFlow(String prefix,String data,DateTime dateTime,String flow,byte[] columnFamily,byte[] column)throws IOException{
        Put put=mPut(prefix,data,dateTime,flow,columnFamily,column);
        return put;
    }

    /**
     * 设置StartRow和EndRow进行scan(千万不能用过滤器)
     * @return
     */
    public ResultScanner scanNetflow(String prefix,Table hTableInterface,String data,long seg,byte[] family) {
        Scan scan = mScan(prefix,data,seg,family);
        scan.setCaching(50);
        scan.setBatch(5);

        ResultScanner resultScanner=null;
        try {
            resultScanner = hTableInterface.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultScanner;
    }

    /**
     * 连接数据表
     * @return
     */
    public Table getHTableInterface(){
        Table hTableInterface=null;
        try {
            connection=ConnectionFactory.createConnection(configuration);
            hTableInterface= connection.getTable(TableName.valueOf(tableName));
        }catch (IOException e){
            System.out.println("表格不存在");
            e.printStackTrace();
        }
        return hTableInterface;
    }

    public Table[] getHTableInterface(int num){
        Table[] res=new Table[num];
        for(int i=0;i<num;i++){
            res[i]=getHTableInterface();
        }
        return res;
    }

    /**
     * 断开数据表的连接
     */
    public void tableClose(Table hTableInterface){
        try {
            hTableInterface.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void tablesClose(Table[] hTableInterfaces){
        for(int i=0;i<hTableInterfaces.length;i++){
            tableClose(hTableInterfaces[i]);
        }
    }
}
