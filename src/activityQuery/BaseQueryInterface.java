package activityQuery;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import tableModel.NetflowInfo;

import java.util.List;

/**
 * 基础的查询接口
 * Created by yangyun on 2017/4/4.
 */
public interface BaseQueryInterface {
    List<NetflowInfo> queryBySrcIPSeg(String srcIP, int mask, String tableName, Table hTableInterface);//地址段查询流记录-源地址

    List<NetflowInfo> queryByDesIPSeg(String desIP, int mask, String tableName, Table hTableInterface);//宿地址段进行查询

    List<NetflowInfo> queryBySrcIP(String srcIP, String tableName, Table hTableInterface);//查询目标IP为源IP的流记录

    List<NetflowInfo> queryByDesIP(String desIP, String tableName, Table hTableInterface);//查询目标IP为宿IP的流记录

    List<NetflowInfo> queryAllNetflow(String IP, int mask,String tableName,Table hTableInterface);//查询IP地址相关的所有流
}
