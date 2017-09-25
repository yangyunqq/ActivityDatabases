package activityQuery;

import org.joda.time.DateTime;
import queryIPCIS.asMap.IP2Manage_Map;
import queryIPCIS.chainLinkWithMap.IP2Lcotion_Map;
import tableModel.NetflowInfo;

import java.util.*;

/**
 * 流记录通信活动分析
 * Created by yangyun on 2016/10/28.
 */
public class AnalysisCommunicationActivity {

    public static List<List<String>> analysisResult(List<NetflowInfo> netflowInfos, String IP, String mode) {

        List<List<String>> result = new ArrayList<List<String>>();//最终返回的结果

        if (netflowInfos.size() == 0) {//没有数据
            return result;
        }

        List<String> resultOfSrcIP = new ArrayList<>();//针对所有通信活动的统计结果
        resultOfSrcIP.add(IP);//目标IP

        Map<String, List<List<String>>> peerMap = new HashMap<String, List<List<String>>>();
        long sumBytes = 0;//字节数
        long sunPKTs = 0;//报文数

        Map<String,Integer> portMap=new HashMap<>();//目标地址使用的端口信息统计

        for (NetflowInfo netflowInfo : netflowInfos) {
            String peerIP = null;//获取对端IP
            if (mode.equals("srcIP")) {
                peerIP = netflowInfo.getDesIP();

                String port=netflowInfo.getSrcPort();
                if(!portMap.containsKey(port)){
                    portMap.put(port,1);
                }else {
                    portMap.put(port,portMap.get(port)+1);
                }
            } else {
                peerIP = netflowInfo.getSrcIP();

                String port=netflowInfo.getDesPort();
                if(!portMap.containsKey(port)){
                    portMap.put(port,1);
                }else {
                    portMap.put(port,portMap.get(port)+1);
                }
            }

            DateTime dateTime = netflowInfo.getDateTime();//通信时间

            List<String> oneCommunication = new ArrayList<String>();//端口信息-源宿

            if (peerMap.get(peerIP) == null) {//对端地址第一次出现
                peerMap.put(peerIP, new ArrayList<List<String>>());
            }
            oneCommunication.add(netflowInfo.getSrcPort());
            oneCommunication.add(netflowInfo.getDesPort());

            String[] infos = netflowInfo.getNetInfo().split("\\s+");

            oneCommunication.add(infos[5]);//报文数
            sunPKTs += Long.valueOf(infos[5]);

            oneCommunication.add(infos[6]);//字节数
            sumBytes += Long.valueOf(infos[6]);

            oneCommunication.add(dateTime.toString());//通信时间
            oneCommunication.add(infos[7]);//TcpFlag

            peerMap.get(peerIP).add(oneCommunication);
        }

        int pereIPNum = peerMap.keySet().size();
        resultOfSrcIP.add(String.valueOf(pereIPNum));//对端IP数量
        resultOfSrcIP.add(String.valueOf(sumBytes / netflowInfos.size()));//平均字节数
        resultOfSrcIP.add(String.valueOf(sunPKTs / netflowInfos.size()));//平均报文数
        resultOfSrcIP.add(String.valueOf(netflowInfos.size()));//流数
        resultOfSrcIP.add(portMap.toString());//添加目标地址使用的端口信息
        result.add(resultOfSrcIP);//目标地址,对端IP数量，流平均字节数，流平均报文数，流数
        //System.out.println("总体分析所需的时间"+(System.currentTimeMillis()-startTIme));//总体概况

        for (Map.Entry<String, List<List<String>>> entry : peerMap.entrySet()) {
            List<String> ResultOfOnePeerIP = new ArrayList<String>();
            ResultOfOnePeerIP.add(IP);//目标IP
            ResultOfOnePeerIP.add(CommonTools.getIPStr(Long.valueOf(entry.getKey())));//对端IP

            int flowNum = entry.getValue().size();//流数
            int PKTs = 0;//报文数
            int Bytes = 0;//字节数

            List<String> srcPorts = new ArrayList<>();//源端口集合
            List<String> desPorts = new ArrayList<>();//宿端口集合
            List<String> times = new ArrayList<>();//通信时间集合
            List<String> tcpFlag=new ArrayList<>();//tcpFlag
            HashMap<String, Integer> tcpFlagMap = new HashMap<>();

            for (List<String> oneCommunication : entry.getValue()) {//同源同宿放的一次通信活动
                srcPorts.add(oneCommunication.get(0));
                desPorts.add(oneCommunication.get(1));
                PKTs += Integer.valueOf(oneCommunication.get(2));
                Bytes += Integer.valueOf(oneCommunication.get(3));
                times.add(oneCommunication.get(4));
                tcpFlag.add(oneCommunication.get(5));//TcpFlag
            }

            for (String flag : tcpFlag) {//出现的次数
                if (!tcpFlagMap.containsKey(flag)) {
                    tcpFlagMap.put(flag, 1);
                } else {
                    tcpFlagMap.put(flag, tcpFlagMap.get(flag) + 1);
                }
            }

            ResultOfOnePeerIP.add(String.valueOf(Bytes / flowNum));//字节数
            ResultOfOnePeerIP.add(String.valueOf(PKTs / flowNum));//报文数
            ResultOfOnePeerIP.add(String.valueOf(flowNum));//流数

            List<String> srcResut = analysisPort(srcPorts);//分析源端口
            List<String> desResult = analysisPort(desPorts);//分析宿端口
            List<String> timeResult = analysisTimes(times);// 分析时间

            ResultOfOnePeerIP.addAll(timeResult);
            ResultOfOnePeerIP.addAll(srcResut);
            ResultOfOnePeerIP.addAll(desResult);

            //TCP——flag信息
            StringBuilder tcp_flag_info=new StringBuilder();
            for(String key:tcpFlagMap.keySet()){
                tcp_flag_info.append(key+"-"+tcpFlagMap.get(key)+" ");
            }
            ResultOfOnePeerIP.add(tcp_flag_info.toString());

            //获取对端IP的相关信息---首先要加载表才可以
            String IP2ManageInfo= IP2Manage_Map.searchTable(Long.valueOf(entry.getKey())).toString();//管理归属信息
            String IP2LocationInfo= IP2Lcotion_Map.searchTable(Long.valueOf(entry.getKey())).toString();//使用位置信息

            ResultOfOnePeerIP.add(IP2ManageInfo);//使用位置信息
            ResultOfOnePeerIP.add(IP2LocationInfo);//管理归属信息

            result.add(ResultOfOnePeerIP);

        }
        return result;
    }

    //端口信息统计[端口数量、去重后端口数、最大端口、最小端口]
    public static List<String> analysisPort(List<String> ports) {
        List<String> result = new ArrayList<>();

        Set<String> setPorts = new HashSet<>(ports);//去重后端口集合
        Map<String, Integer> map = new HashMap<>();//端口出现次数统计

        int MinPort = Integer.MAX_VALUE;//最小端口
        int MaxPort = Integer.MIN_VALUE;//最大端口
        for (String port : ports) {
            MinPort = (MinPort > Integer.valueOf(port)) ? Integer.valueOf(port) : MinPort;
            MaxPort = (MaxPort < Integer.valueOf(port)) ? Integer.valueOf(port) : MaxPort;
        }

        //统计端口出现的次数
        for (String port : ports) {
            if (map.get(port) == null) {
                map.put(port, 1);
            } else {
                map.put(port, map.get(port) + 1);
            }
        }

        String portAppearMax = String.valueOf(MinPort);
        int num = 0;
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (entry.getValue() > num) {
                portAppearMax = entry.getKey();
                num = entry.getValue();
            }
        }

        result.add(String.valueOf(ports.size()));//端口数
        result.add(String.valueOf(setPorts.size()));//去重后端口数
        result.add(String.valueOf(MaxPort));
        result.add(String.valueOf(MinPort));
        result.add(String.valueOf(portAppearMax)+"-"+String.valueOf(num));

        return result;
    }

    //分析通信活动时间[起始通信时间,结束通信时间,平均通信时间间隔]
    public static List<String> analysisTimes(List<String> times) {
        List<String> result = new ArrayList<>();

        List<Long> timesNum = new ArrayList<>();
        for (String string : times) {
            timesNum.add(new DateTime(string).getMillis());
        }

        Collections.sort(timesNum);

        int count = timesNum.size();
        DateTime startTime = new DateTime(timesNum.get(0));//启示时间
        DateTime endTime = new DateTime(timesNum.get(count - 1));//结束时间

        int interval = 0;//通信时间间隔
        int sum = 0;
        if (count > 1) {
            for (int i = 1; i < count; i++) {
                sum += (timesNum.get(i) - timesNum.get(i - 1));
            }
            interval = sum / (count - 1);
        }
        result.add(startTime.toString("yyyy-MM-dd HH:mm:ss"));
        result.add(endTime.toString("yyyy-MM-dd HH:mm:ss"));

        result.add(String.valueOf(interval/1000));//秒级别
        return result;
    }

    //分析结果进行输出
    public static String mergeAnalysisInfo(List<List<String>> analysisResult,String mode){
        if(analysisResult.size()==0){
            System.out.println("无相关通信信息");
            return "";
        }

        List<String> first=analysisResult.get(0);
        StringBuilder stringBuilder=new StringBuilder();
        String role=null;
        if(mode.equals("srcIP")){
            role="源地址";
        }else{
            role="宿地址";
        }
        //stringBuilder.append("目标IP:"+first.get(0)+"\t目标IP角色:"+role+"\t对端IP数量:"+first.get(1)+"\t流平均字节数:"+first.get(2)+"\t流平均报文数:"+first.get(3)+"\t流数:"+first.get(4)+"\n");
        /*stringBuilder.append(
                "对端IP"+
                        "\t平均字节数"+
                        "\t平均报文数"+
                        "\t流数"+
                        //"\t起始时间"+
                        //"\t结束时间"+
                        //"\t平均时间间隔(秒)"+
                        //"\t源端口数量"+
                        //"\t去重后端口数量"+
                        //"\t最大源端口"+
                        //"\t最小源端口"+
                        "\t出现次数最多源端口及其数量"+
                        //"\t宿端口数量"+
                        //"\t去重后宿端口数量"+
                        //"\t最大宿端口"+
                        //"\t最小宿端口"+
                        "\t出现次数最多的宿端口及其数量"+
                        "\tTcpFlag信息"+
                        "\t归属信息"+
                        "\t使用位置信息"+"\n"
        );*/

        stringBuilder.append(first.get(0)+"$"+role+"$"+first.get(1)+"$"+first.get(2)+"$"+first.get(3)+"$"+first.get(4)+ "%");
        int index=1;
        while(index<analysisResult.size()){
            List<String> oneline=analysisResult.get(index);
            /*
            stringBuilder.append(
                    oneline.get(1)+
                    "\t"+oneline.get(2)+
                    "\t"+oneline.get(3)+
                    "\t"+oneline.get(4)+
                    //"\t"+oneline.get(5)+
                    //"\t"+oneline.get(6)+
                    //"\t"+oneline.get(7)+
                    //"\t"+oneline.get(8)+
                    //"\t"+oneline.get(9)+
                    //"\t"+oneline.get(10)+
                    //"\t"+oneline.get(11)+
                    "\t"+oneline.get(12)+
                    //"\t"+oneline.get(13)+
                    //"\t"+oneline.get(14)+
                    //"\t"+oneline.get(15)+
                    //"\t"+oneline.get(16)+
                    "\t"+oneline.get(17)+
                    "\t"+oneline.get(18)+
                    "\t"+oneline.get(19)+
                    "\t"+oneline.get(20)+ "\n"
            );*/

            stringBuilder.append(
                    oneline.get(1)+
                            "$"+oneline.get(2)+
                            "$"+oneline.get(3)+
                            "$"+oneline.get(4)+
                            //"\t"+oneline.get(5)+
                            //"\t"+oneline.get(6)+
                            //"\t"+oneline.get(7)+
                            //"\t"+oneline.get(8)+
                            //"\t"+oneline.get(9)+
                            //"\t"+oneline.get(10)+
                            //"\t"+oneline.get(11)+
                            "$"+oneline.get(12)+
                            //"\t"+oneline.get(13)+
                            //"\t"+oneline.get(14)+
                            //"\t"+oneline.get(15)+
                            //"\t"+oneline.get(16)+
                            "$"+oneline.get(17)+
                            "$"+oneline.get(18)+
                            "$"+oneline.get(19)+
                            "$"+oneline.get(20)+ "%"
            );
            index++;
        }
        return stringBuilder.toString();
    }
}

