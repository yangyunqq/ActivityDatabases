package activityInsert;

import netflowAnalysis.DealNetFlow;
import netflowAnalysis.Netflow;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import tableModel.NetflowInfoDAO;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 活动库数据插入模块
 * Created by yangyun on 16/9/11.
 */
public class NetflowInputHBase {
    //HBase表格相关常量
    private String tableName;
    private NetflowInfoDAO netflowInfoDAO;
    public boolean flag;
    public String dateTime;//对象的有效生存时间,一天
    public boolean InputIsdown=true;//为了提前终止进程
    public int countInterrupt=0;//统计手动停止线程的次数

    //netflow缓冲区
    private Vector<String> netflowCatch1=new Vector<String>();
    private Vector<String> netflowCatch2=new Vector<String>();

    //相关常量
    private int THREADNUM;
    long timeCountMinute=30;//缓冲时间-秒
    long timeCountMsec=timeCountMinute*1000;//缓冲时间-毫秒

    public NetflowInputHBase(String tableName,int THREADNUM,String dateTime){
        this.tableName=tableName;
        netflowInfoDAO=new NetflowInfoDAO(Bytes.toBytes(tableName));
        this.THREADNUM=THREADNUM;
        this.dateTime=dateTime;
        flag=true;
    }

    public NetflowInfoDAO getNetflowInfoDAO(){
        return netflowInfoDAO;
    }

    /**
     * 单线程缓存timeCount时间的流记录,并计算数据到达的速度
     * @param datagramSocket
     */
    public void netflowToMemory(DatagramSocket datagramSocket){
        final int BUFFSIZR = 2048;
        byte[] recvBuf = new byte[BUFFSIZR];
        DealNetFlow dealNetFlow = new DealNetFlow();
        DatagramPacket packet=new DatagramPacket(recvBuf,BUFFSIZR);
        long startTime=System.currentTimeMillis();//接收的开始时间

        while(System.currentTimeMillis()-startTime<timeCountMsec) {
            try {
                datagramSocket.receive(packet);
                if (recvBuf.length == 0) {
                    System.out.println("缓冲区为空,没有netflow数据到达");
                    System.out.println("地址 : " + datagramSocket.getInetAddress());
                    System.out.println("端口 : " + datagramSocket.getPort());
                }

                Netflow netflow = new Netflow(recvBuf);
                List<Map<String, Long>> res = dealNetFlow.analysisNetflow(netflow);
                if (res == null) {
                    //System.out.println("模版流");
                    continue;
                }
                for (Map<String, Long> map : res) {
                    if (map == null || map.size() == 0) {
                        System.out.println("数据流未被解析");
                        break;
                    }

                    String oneNetflow = map.get("SRC_ADDR") + " " +
                            map.get("DST_ADDR") + " " +
                            map.get("SRC_PORT") + " " +
                            map.get("DST_PORT") + " " +
                            map.get("NEXT_HOP") + " " +
                            map.get("PROTOCOL") + " " +
                            map.get("LAST_SWITCHED") + " " +
                            map.get("FIRST_SWITCHED") + " " +
                            map.get("DIRECTION") + " " +
                            map.get("IN_PKTS") + "  " +
                            map.get("IN_BYTES") + "  " +
                            map.get("TCP_FLAGS") + "  " +
                            map.get("TOS") + "  " +
                            map.get("SRC_MASK") + "  " +
                            map.get("DST_MASK") + " " +
                            map.get("SRC_AS") + " " +
                            map.get("DST_AS") + " " +
                            map.get("BGP_NEXT_HOP") + '\n';//这里的这个换行是多余的
                    //System.out.println(oneNetflow);//debug
                    netflowCatch1.add(oneNetflow);
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    /**
     * 这里使用两个线程对流记录进行缓存--理由接受到的Netflow通过两个端口发过来
     */
    public void NetflowToMemoryThread(final DatagramSocket serverSocket1, final DatagramSocket serverSocket2, final int cacheSize){
        while(flag){
            Thread[] threads=new Thread[2];
            threads[0]=new Thread(new Runnable() {
                @Override
                public void run() {
                    netflowToMemory(serverSocket1);
                }
            });

            threads[1]=new Thread(new Runnable() {
                @Override
                public void run() {
                    netflowToMemory(serverSocket2);
                }
            });

            threads[0].start();
            threads[1].start();

            try {
                threads[0].join();
                threads[1].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

/*
            for(int  i=0;i<threads.length;i++){
                try {
                    threads[i].join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
*/
            /**缓冲区缓冲了30秒的数据,可以对数据进行处理
             比如:数据组流,数据压缩
             目前没有额外操作直接对数据进行插入
            */
            long netflowInputSpeed=netflowCatch1.size()/timeCountMinute;//数据到达速度
            InputData_Main.count+=netflowCatch1.size();//条数累加

            netflowCatch2.clear();//需要测试
            netflowCatch2=null;

            netflowCatch2=netflowCatch1;
            netflowCatch1=new Vector<>();

            //判断数据插入是否正常完成
            if(InputIsdown==false){
                //插入线程没有执行完成
                System.out.println("手动停止线程"+(++countInterrupt));
                netflowCatch2.clear();
                try {
                    InputData_Main.out.write(" HBaseInput unsuccessfully\n");
                    InputData_Main.out.flush();
                    System.exit(0);//强制终止程序
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            //把数据到达速度写入文件
            String time =new DateTime().toString("yy-MM-dd HH:mm:ss EE").split("\\s+")[1];
            try {
                InputData_Main.out.write(time);
                InputData_Main.out.write("  NetflowInput:"+netflowInputSpeed);
                InputData_Main.out.flush();
            } catch (IOException e) {
                System.out.println("写入速度记录程序出错...");
                e.printStackTrace();
            }

            //开始数据插入
            InputIsdown=false;
            Thread InputThread=new Thread(new Runnable() {
                @Override
                public void run() {
                    MemoryToHBase(netflowCatch2,THREADNUM,cacheSize);//数据插入速度
                }
            });
            InputThread.start();

            //判断是否进入了新的一天
            String currentDateTime ="20"+ new DateTime().toString("yy-MM-dd HH:mm:ss EE").split("\\s+")[0];

            if(!currentDateTime.equals(dateTime)){
                //说明是新的一天了
                flag=false;
                try {
                    try {
                        //等待当天最后一个缓冲区的数据写入数据库
                        InputThread.join();

                        //进行数据老化判断
                        Thread dataAging=new Thread(new Runnable() {
                            @Override
                            public void run() {
                                DeleteNetflow deleteNetflow=new DeleteNetflow();
                                deleteNetflow.dataAging();
                            }
                        });
                        dataAging.start();
                        dataAging.join();
                    }catch (InterruptedException e){
                        e.printStackTrace();
                    }

                    InputData_Main.out.write("count:"+InputData_Main.count+"\n");
                    InputData_Main.out.flush();
                    InputData_Main.out.close();

                    //断开连接
                    for(int i=0;i<InputData_Main.pools.length;i++){
                        InputData_Main.pools[i].close();
                    }
                }catch(IOException e){
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * netflow数据从缓存到数据库
     * 先对缓存进行分块
     * 每个线程读取--分层
     * @return 多线程插入速度
     */
    public void MemoryToHBase(final Vector<String> netflowCache, int threadNum, final int cacheSize){
        /*人工设置延迟
        if(count%9==0){
            try {
                TimeUnit.SECONDS.sleep(28);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        count++;*/

        Thread[] threads=new Thread[threadNum];

        int blockSize;
        final int cacheLength=netflowCache.size();
        blockSize=cacheLength/threadNum;


        long startTime=System.currentTimeMillis();//

        final int[] indexStart=new int[threadNum];
        final int[] indexEnd=new int[threadNum];

        indexEnd[0]=0;
        indexEnd[0]=blockSize;
        for(int i=1;i<threadNum-1;i++){
            indexStart[i]=indexEnd[i-1]+1;
            indexEnd[i]=indexStart[i]+blockSize;
        }
        indexStart[threadNum-1]=indexEnd[threadNum-2]+1;
        indexEnd[threadNum-1]=cacheLength-1;

        for(int i=0;i<threadNum;i++){
            final int index=i;
            threads[i]=new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        InputData_Main.pools[index].setAutoFlush(false);
                        InputData_Main.pools[index].setWriteBufferSize(cacheSize * 1024 * 1024);//设置6MB的缓冲区
                        inputIntoHBase(InputData_Main.pools[index], netflowCache, indexStart[index], indexEnd[index]);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });

            threads[i].setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    System.out.println("写入线程出错");
                    e.printStackTrace();
                }
            });
            threads[i].setDaemon(true);//设置为后台线程
            threads[i].start();
        }

        for(Thread thread:threads){
            try {
                thread.join();
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }

        //提前终止数据插入
        if(InputIsdown==true) {
            System.out.println("提前终止");
            netflowCache.clear();
            return;
        }

        long timeCost=System.currentTimeMillis()-startTime;//必须小于30秒
        long time=timeCost/1000;//秒级别
        if(time==0){
            time=1;
        }
        long dataInpueSpeed=cacheLength/time;
        try {
            InputData_Main.out.write("  HBaseInput: "+dataInpueSpeed+"\n");
            InputData_Main.out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        netflowCache.clear();//清空
        InputIsdown=true;
    }

    public void inputIntoHBase(HTableInterface userTable, Vector<String> netflowCach, int left, int right)throws IOException{
        List<Put> netflowPut=new ArrayList<Put>();
        for(int i=left;(i<right)&&(InputIsdown==false);i++){
            String flow=netflowCach.get(i);
            String[] flowFields=flow.split(" ",5);
            DateTime dateTime=new DateTime(System.currentTimeMillis());//当前系统时间

            /**
             * 注意这里应该需要修改，考虑数据写入的原子性
             */
            netflowPut.add(netflowInfoDAO.putOneFlow("A",flowFields[0],dateTime,flow,NetflowInfoDAO.SrcIPIndexInfo,NetflowInfoDAO.info));//源地址索引数据
            netflowPut.add(netflowInfoDAO.putOneFlow("B",flowFields[1],dateTime,flow,NetflowInfoDAO.DesIPIndexInfo,NetflowInfoDAO.info));//宿地址索引数据
            //netflowPut.add(netflowInfoDAO.putOneFlow("C",flowFields[2],dateTime,flow,NetflowInfoDAO.SrcPortIndexInfo,NetflowInfoDAO.info));//源端口索引数据
            //netflowPut.add(netflowInfoDAO.putOneFlow("D",flowFields[3],dateTime,flow,NetflowInfoDAO.DesPortIndexInfo,NetflowInfoDAO.info));//宿端口索引数据
        }
        userTable.put(netflowPut);//真正插入数据的地方
    }
}