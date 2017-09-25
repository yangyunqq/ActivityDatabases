package netflowAnalysis;

import java.util.*;

/**
 * @author yangyun 2016-09-16
 * @version 1
 * netflow中的一个字段
 */
class OneFiled {
    /**字段名称*/
    int filedName;
    /**字段所占的长度*/
    int length;
    OneFiled(int filedName,int length){
        this.filedName=filedName;
        this.length=length;
    }
}

/**
 * 解析netflow的工具类
 */
public class DealNetFlow {
    public final List<OneFiled> EMPTYTEMPLATE=new LinkedList<>();//模版的属性为空---用来表示目前的数据流没有对应的模版
    private Map<Integer,String> map=new HashMap<Integer, String>();//用hashMap存储每条流记录要保存的属性
    public Map<Integer,List<OneFiled>> templateLIst=null;//存储新的模版类(使用HashMap自动的进行去重)

    public DealNetFlow(){
        map.put(new Integer(1),new String("IN_BYTES"));         //入方向的IP流长度
        map.put(new Integer(2),new String("IN_PKTS"));          //入方向的IP数据包数量
        map.put(new Integer(4),new String("PROTOCOL"));         //6-TCP 17-UDP(传输层协议)
        map.put(new Integer(5),new String("TOS"));              //服务类型
        map.put(new Integer(6),new String("TCP_FLAGS"));
        map.put(new Integer(7),new String("SRC_PORT"));         //源端口
        map.put(new Integer(8),new String("SRC_ADDR"));         //源IP
        map.put(new Integer(9),new String("SRC_MASK"));
        map.put(new Integer(11),new String("DST_PORT"));        //目的端口
        map.put(new Integer(12),new String("DST_ADDR"));        //目的地址
        map.put(new Integer(13),new String("DST_MASK"));
        map.put(new Integer(15),new String("NEXT_HOP"));
        map.put(new Integer(16),new String("SRC_AS"));          //源AS
        map.put(new Integer(17),new String("DST_AS"));          //目的AS
        map.put(new Integer(18),new String("BGP_NEXT_HOP"));    //BGP下一条路由
        map.put(new Integer(21),new String("LAST_SWITCHED"));   //终止时间
        map.put(new Integer(22),new String("FIRST_SWITCHED"));  //起始时间
        map.put(new Integer(61),new String("DIRECTION"));       //流向

        templateLIst=new HashMap<Integer,List<OneFiled>>();//存储新的模版类(使用HashMap自动的进行去重)

        //把常见的模版预先添加到容器中
        //1315模版
        templateLIst.put(1315,new ArrayList<OneFiled>(Arrays.asList(
                new OneFiled(8,4),
                new OneFiled(12,4),
                new OneFiled(15,4),
                new OneFiled(2,4),
                new OneFiled(1,4),
                new OneFiled(22,4),
                new OneFiled(21,4),
                new OneFiled(18,4),
                new OneFiled(10,4),
                new OneFiled(14,4),
                new OneFiled(7,2),
                new OneFiled(11,2),
                new OneFiled(16,2),
                new OneFiled(17,2),
                new OneFiled(58,2),
                new OneFiled(59,2),
                new OneFiled(232,2),
                new OneFiled(6,1),
                new OneFiled(4,1),
                new OneFiled(5,1),
                new OneFiled(9,1),
                new OneFiled(13,1),
                new OneFiled(61,1),
                new OneFiled(89,1),
                new OneFiled(210,3)
        )));
        //1317模版
        templateLIst.put(1317,new ArrayList<OneFiled>(Arrays.asList(
                new OneFiled(8,4),
                new OneFiled(12,4),
                new OneFiled(15,4),
                new OneFiled(2,4),
                new OneFiled(1,4),
                new OneFiled(22,4),
                new OneFiled(21,4),
                new OneFiled(18,4),
                new OneFiled(47,4),
                new OneFiled(10,4),
                new OneFiled(14,4),
                new OneFiled(7,2),
                new OneFiled(11,2),
                new OneFiled(16,2),
                new OneFiled(17,2),
                new OneFiled(58,2),
                new OneFiled(59,2),
                new OneFiled(232,2),
                new OneFiled(9,1),
                new OneFiled(13,1),
                new OneFiled(6,1),
                new OneFiled(4,1),
                new OneFiled(5,1),
                new OneFiled(70,3),
                new OneFiled(71,3),
                new OneFiled(72,3),
                new OneFiled(73,3),
                new OneFiled(46,1),
                new OneFiled(61,1),
                new OneFiled(89,1),
                new OneFiled(210,2)
        )));
        //1320模版
        templateLIst.put(1320,new ArrayList<OneFiled>(Arrays.asList(
                new OneFiled(8,4),
                new OneFiled(12,4),
                new OneFiled(2,4),
                new OneFiled(1,4),
                new OneFiled(22,4),
                new OneFiled(21,4),
                new OneFiled(10,4),
                new OneFiled(14,4),
                new OneFiled(58,2),
                new OneFiled(59,2),
                new OneFiled(4,1),
                new OneFiled(9,1),
                new OneFiled(13,1),
                new OneFiled(61,1),
                new OneFiled(89,1),
                new OneFiled(210,3)
        )));
        //1319模版
        templateLIst.put(1319,new ArrayList<OneFiled>(Arrays.asList(
                new OneFiled(2,4),
                new OneFiled(1,4),
                new OneFiled(22,4),
                new OneFiled(21,4),
                new OneFiled(47,4),
                new OneFiled(10,4),
                new OneFiled(14,4),
                new OneFiled(58,2),
                new OneFiled(59,2),
                new OneFiled(232,2),
                new OneFiled(70,3),
                new OneFiled(71,3),
                new OneFiled(72,3),
                new OneFiled(73,3),
                new OneFiled(46,1),
                new OneFiled(61,1)
        )));
    }

    /**
     * 处理接受到的一条流记录
     * @param netflow 接受到的流记录
     * @return
     */
    public List<Map<String,Long>> analysisNetflow(Netflow netflow){
        if(netflow.isTemplate()) {
            //是模版流
            analysisTemplate(netflow);
            return null;
        }
        else {
            //数据流
            List<Map<String,Long>> dataFlow=analysisDataFlow(netflow);
            return dataFlow;
        }
    }

    /**
     * 解析模版流并存入容器--template
     * @param templateFlow 一条模版流
     */
    public void analysisTemplate(Netflow templateFlow){
        if(templateFlow.isTemplate()){
            //是模版流
            List<OneFiled> FiledList=new LinkedList<>();//存储一条模版流的所有属性
            int templateID=templateFlow.unsignedShort(templateFlow.Data,24);//获取模版号
            int count=templateFlow.unsignedShort(templateFlow.Data,26);//模版流有多少个属性
            int index=28;
            for(int i=0;i<count;i++){
                /**
                 * 解析模版的字段
                 */
                Integer field_name=templateFlow.unsignedShort(templateFlow.Data,index);
                index+=2;//取下一个数据
                Integer length=templateFlow.unsignedShort(templateFlow.Data,index);
                index+=2;
                OneFiled oneFiled=new OneFiled(field_name,length);
                FiledList.add(oneFiled);
            }
            //存储模版
            templateLIst.put(templateID,FiledList);
        }
    }

    /**
     * 根据数据流的flowSetID查找对应的Template
     * @param dataFlow 一条数据流
     * @return 模版流
     */
    public List<OneFiled> searchTemplate(Netflow dataFlow){
        int dataFlowsetID=dataFlow.unsignedShort(dataFlow.Data,20);//获取DataFLow的ID号
        if (templateLIst.containsKey(dataFlowsetID))//当前数据流有对应的模版
            return templateLIst.get(dataFlowsetID);
        else
            return EMPTYTEMPLATE;//找不到模版
    }

    /**
     * 解析数据流(一个DataFlow中有多条流记录)
     * @param dataFlow 一条数据流
     * @return 数据流集合
     */
    public List<Map<String, Long>> analysisDataFlow(Netflow dataFlow){
        int length=dataFlow.unsignedShort(dataFlow.Data,22);
        List<OneFiled> Tempplate=new LinkedList<>(searchTemplate(dataFlow));//数据流对应的模版

        //if(dataFlow.Data.length==0) return;

        if(!Tempplate.equals(EMPTYTEMPLATE)) {//有对应的模版
            int sizeOfOneFlow = 0;//一条数据流的字节长度
            for (OneFiled fild : Tempplate)
                sizeOfOneFlow += fild.length;
            int countFlow = (length - 4) / sizeOfOneFlow;//流数

            int index = 24;

            List<Map<String,Long>> res= new LinkedList<>();//存储解析结果
            for (int i = 0; i < countFlow; i++) {
                Map<String, Long> dataFlowAnalysised = new HashMap<>();//存储解析后的数据流(一条)

                for (OneFiled filed : Tempplate) {
                    long value = dataFlow.getValueByBytes(dataFlow.Data, index, filed.length);
                    index += filed.length;
                    if (map.containsKey(filed.filedName))//需要保留该属性
                        dataFlowAnalysised.put(map.get(filed.filedName), new Long(value));
                }
                res.add(dataFlowAnalysised);
            }
            return res;
        }
        return null;
    }
}
