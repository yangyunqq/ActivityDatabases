package activityQuery;

/**
 * 数据处理相关的工具类
 * Created by yangyun on 2017/4/4.
 */
public class CommonTools {
    /**
     * 把IP地址转换为数字形式
     * @param IPString
     * @return
     */
    public static long getIPNum(String IPString){
        long res=0;
        String[] arrays=IPString.split("\\.");
        StringBuilder prefix=new StringBuilder();

        for(int i=0;i<4;i++){
            String temp=Integer.toBinaryString(Integer.parseInt(arrays[i]));
            StringBuilder stringBuilder=new StringBuilder(temp);//stringbuilder中存储的是8位二进制数
            int templength=stringBuilder.length();
            if(stringBuilder.length()<8){
                for(int j=0;j<8-templength;j++){
                    stringBuilder.insert(0,'0');
                }
            }
            prefix.append(stringBuilder);
        }
        for(int i=0;i<32;i++){
            res+=Integer.parseInt(new String(String.valueOf(prefix.charAt(i))))*Math.pow(2,31-i);
        }
        return res;
    }

    /**
     * 把整数IP地址转换为点分式
     * @param IPNum
     * @return
     */
    public static String getIPStr(long IPNum){
        return new StringBuilder().append(((IPNum >> 24) & 0xff)).append('.')
                .append((IPNum >> 16) & 0xff).append('.').append(
                        (IPNum >> 8) & 0xff).append('.').append((IPNum & 0xff))
                .toString();
    }
}
