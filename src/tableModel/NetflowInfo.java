package tableModel;

import activityQuery.CommonTools;
import netflowAnalysis.Netflow;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;

import java.util.List;

/**
 * 数据库中的一条netflow信息
 * Created by yangyun on 16/9/11.
 */
public class NetflowInfo{
    private String srcIP;
    private String desIP;
    private String srcPort;
    private String desPort;
    private String netInfo;
    private DateTime dateTime;

    public NetflowInfo(String srcIP,String desIP,String srcPort,String desPort,String netInfo,DateTime dateTime){
        this.srcIP=srcIP;
        this.desIP=desIP;
        this.srcPort=srcPort;
        this.desPort=desPort;
        this.netInfo=netInfo;
        this.dateTime=dateTime;
    }

    public NetflowInfo(Result result,byte[] columnFamily){
        String flow=Bytes.toString(result.getValue(columnFamily,NetflowInfoDAO.info));
        String[] field=flow.split(" ",5);
        this.srcIP=field[0];
        this.desIP=field[1];
        this.srcPort=field[2];
        this.desPort=field[3];
        this.netInfo=field[4];
        this.dateTime=new DateTime(result.raw()[0].getTimestamp());
    }

    public String getSrcIP(){
        return srcIP;
    }

    public String getDesIP(){
        return desIP;
    }

    public String getSrcPort(){
        return srcPort;
    }

    public String getDesPort(){
        return desPort;
    }

    public String getNetInfo(){
        return netInfo;
    }

    public DateTime getDateTime(){
        return dateTime;
    }

    @Override
    public String toString(){
        return String.format("SrcIP:%s,DesIP:%s,SrcPort:%s,DesPort:%s,netInfo:%s,DateTime:%s",srcIP,desIP,srcPort,desPort,netInfo,dateTime);
    }

    /**
     * 流记录的输出格式 各个字段之间使用空格分开
     * @return
     */
    public String mergeNetflow(){
        StringBuilder res=new StringBuilder(CommonTools.getIPStr(Long.parseLong(srcIP))+" "+CommonTools.getIPStr(Long.parseLong(desIP))+" "+srcPort+" "+desPort+" ");
        String[] netflowInfo=netInfo.split("\\s+");
        for(String str:netflowInfo){
            res.append(str+" ");
        }
        res.append(dateTime.toString("yyyy-MM-dd hh:mm:ss")+"\n");
        return res.toString();
    }

    /**
     * 多条流记录之间按照"\n"分开
     * @return
     */
    public static String mergeMutipliNetflow(List<NetflowInfo> netflowInfos){
        StringBuilder res=new StringBuilder();
        for(NetflowInfo netflowInfo:netflowInfos){
            res.append(netflowInfo.mergeNetflow());
        }
        return res.toString();
    }
}
