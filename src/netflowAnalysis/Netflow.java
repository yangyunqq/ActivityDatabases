package netflowAnalysis;

/**
 * @author yangyun
 * @version 1
 * 一条NetFlow原始流记录
 */

public class Netflow {
    public byte[] Data = null;
    public int FlowSet;//FlowSet区分NetFlow是模版流还是数据流(小于256是模版)
    public int templateID;//模版ID

    public Netflow(byte[] data){
        Data=data;//接收到的字节流记录
        FlowSet=getFlowSetID(Data);
        templateID=getTemolateID(Data);
    }

    /**
     * 取两个字节的数据组合成一个整数
     */
    public static int unsignedShort(byte[] data, int index) {
        return (int) (((short) (data[index] & 0xFF) << 8) + (short) (data[index + 1] & 0xFF));
    }

    /**
     * 从index开始取n个字节最后转换为整数
     */
    public static long getValueByBytes(byte[] Data,int index,int n){
        long result=0;
        if(n==1)
            return Data[index]&0xFF;
        else {
            for(int i=0;i<n;i++){
                result+=(long)(Data[index+i] & 0xFF)<<((n-i-1)*8);
            }
            return result;
        }
    }

    /**
     * 获取NetFlow的ID(第20个字节)--FlowSet
     */
    public static int getFlowSetID(byte[] Data){
        return unsignedShort(Data,20);
    }

    /**
     *判断一条NetFlow是不是模版流
     */
    public boolean isTemplate(){
        if(FlowSet<256)
            return true;
        else
            return false;
    }

    /**
     * 获取模版流的TemplateID
     */
    public int getTemolateID(byte[] Data){
        if(isTemplate()==true)
            return unsignedShort(Data,24);
        else
            return -1;
    }
}