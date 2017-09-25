package activityQuery;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.RubyProcess;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;
import tableModel.NetflowInfo;
import tableModel.NetflowInfoDAO;
import java.util.ArrayList;
import java.util.List;


/**
 * 查询接口的实现类
 * 1.单线程
 * 2.
 * Created by yangyun on 2017/4/4.
 */
public class QueryImpl implements BaseQueryInterface{
    /**
     * 单线程进程数据查询
     * @param prefix
     * @param data
     * @param mask
     * @param tableName
     * @param hTableInterface
     * @param family
     * @return
     */
    public ResultScanner queryByRowKeySingleThread(
            final String prefix,
            final String data,
            int mask,
            String tableName,
            final Table hTableInterface,
            final byte[] family) {
        final long seg=(long) Math.pow(2,32-mask);
        final NetflowInfoDAO netflowInfoDAO=new NetflowInfoDAO(Bytes.toBytes(tableName));
        ResultScanner resultScanner=netflowInfoDAO.scanNetflow(prefix,hTableInterface,data,seg,family);
        return resultScanner;
    }

    /**
     * 根据源IP地址段查询相关的流记录
     * @param srcIP
     * @param mask
     * @param tableName
     * @param hTableInterface
     * @return
     */
    @Override
    public List<NetflowInfo> queryBySrcIPSeg(
            String srcIP,
            int mask,
            String tableName,
            Table hTableInterface) {
        ResultScanner resultScanner=queryByRowKeySingleThread("A",srcIP,mask,tableName,hTableInterface,NetflowInfoDAO.SrcIPIndexInfo);
        List<NetflowInfo> res=new ArrayList<>();
        for(Result reslut:resultScanner){
            res.add(new NetflowInfo(reslut,NetflowInfoDAO.SrcIPIndexInfo));
        }
        //筛选
        res=filtNetflow(srcIP,mask,"srcIP",res);

        return res;
    }

    @Override
    public List<NetflowInfo> queryByDesIPSeg(
            String desIP,
            int mask,
            String tableName,
            Table hTableInterface) {
        ResultScanner resultScanner=queryByRowKeySingleThread("B",desIP,mask,tableName,hTableInterface,NetflowInfoDAO.DesIPIndexInfo);
        List<NetflowInfo> res=new ArrayList<>();
        for(Result reslut:resultScanner){
            res.add(new NetflowInfo(reslut,NetflowInfoDAO.DesIPIndexInfo));
        }
        //筛选
        res=filtNetflow(desIP,mask,"desIP",res);

        return res;
    }

    @Override
    public List<NetflowInfo> queryBySrcIP(String srcIP, String tableName, Table hTableInterface) {
        return queryBySrcIPSeg(srcIP,32,tableName,hTableInterface);
    }


    @Override
    public List<NetflowInfo> queryByDesIP(String desIP, String tableName, Table hTableInterface) {
        return queryByDesIPSeg(desIP,32,tableName,hTableInterface);
    }

    @Override
    public List<NetflowInfo> queryAllNetflow(String IP, int mask,String tableName, Table hTableInterface) {
        List<NetflowInfo> srcIPNetflow= queryBySrcIPSeg(IP,mask,tableName,hTableInterface);
        List<NetflowInfo> desIPNetflow= queryBySrcIPSeg(IP,mask,tableName,hTableInterface);
        srcIPNetflow.addAll(desIPNetflow);
        return srcIPNetflow;
    }

    /**
     * 前缀匹配有问题,eg:1.1.1.1 会查询出 10.10.10.10的地址
     * @param IP
     * @param mask
     * @param role 源地址还是宿地址
     * @return
     */
    private List<NetflowInfo> filtNetflow(String IP,int mask,String role,List<NetflowInfo> netflowInfos){
        List<NetflowInfo> res=new ArrayList<>();

        //地址位数不到10
        if(String.valueOf(CommonTools.getIPNum(IP)).length()!=10){
            //获取地址段的开始地址和结束地址
            long startIPNum=CommonTools.getIPNum(IP);
            long endIPNum=startIPNum+(long) Math.pow(2,32-mask);


            String IPnum=String.valueOf(CommonTools.getIPNum(IP));//数字形式的地址
            //可能会有错误数据输出,单个地址查询
            for(NetflowInfo netflowInfo:netflowInfos){
                String ip=null;
                if(role=="srcIP"){
                    ip=netflowInfo.getSrcIP();
                } else if(role=="desIP"){
                    ip=netflowInfo.getDesIP();
                }
                long ipNum=Long.valueOf(ip);//流记录中IP地址的数字形式
                if(ipNum>=startIPNum&&ipNum<=endIPNum){//判断地址是否合法
                    res.add(netflowInfo);
                }
            }
            return res;
        }else{
            return netflowInfos;//不用过滤
        }
    }
}
