package activityQuery;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import tableModel.NetflowInfo;
import tableModel.NetflowInfoDAO;
import java.io.*;
import java.net.HttpURLConnection;
import java.util.*;
import java.util.concurrent.*;

/**
 * 具体的处理查询请求的类
 * 查询类型说明
 * mode-1:Chairs查询请求
 * mode-2:通用的流记录查询
 * Created by yangyun on 2017/5/12.
 */
public class QueryHttphandler implements HttpHandler{
    private static int taskNum=5;//科并行查询的任务数


    private QueryImpl baseQueryInterface=new QueryImpl();
    private static File queryMode1_log=new File("/home/hadoop/yangyun/QueryMode1CostTime.log");//记录mode=1的查询任务花费的时间
    private static File queryMode2_log=new File("/home/hadoop/yangyun/QueryMode2CostTime.log");//记录mode=2的查询任务花费的时间
    public  static BufferedWriter queryMode1Out=null;
    public  static BufferedWriter queryMode2Out=null;

    static {
        try {
            queryMode1Out=new BufferedWriter(new FileWriter(queryMode1_log,true));
            queryMode2Out=new BufferedWriter(new FileWriter(queryMode2_log,true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        String httpMethod = httpExchange.getRequestMethod();//请求方式
        String requestURI = httpExchange.getRequestURI().toString();//请求的URL

        if (httpMethod.equalsIgnoreCase("POST") && requestURI.equals("/IPCIS/activityDatabase")) {
            //System.out.println("POST");
            //System.out.println(requestURI);
            new PostQueryHandler().handle(httpExchange);
        } else {
            //System.out.println("Get");
            //System.out.println(requestURI);
            new GetQueryHandler().handle(httpExchange);
        }
    }

    /**
     * 解析HTTP请求的参数
     * @param input 输入的字符串
     * @return
     */
    public Map<String,String> getParams(String input){
        Map<String,String> res=new HashMap<>();
        String[] params=input.split("&");
        System.out.println(input);
        res.put("IpSets",params[0].split("=")[1]);
        res.put("TableName",params[1].split("=")[1]);
        res.put("Mode",params[2].split("=")[1]);

        return res;
    }

    /**
     * 根据参数获取信息
     * @param ipSets
     * @param tableName
     * @param mode
     * @return
     * @throws IOException
     */
    private StringBuffer getInfoFromActivityDataBase(String ipSets,final String tableName,final String mode) throws IOException {
        final StringBuffer res=new StringBuffer();
        NetflowInfoDAO netflowInfoDAO=new NetflowInfoDAO(Bytes.toBytes(tableName));

        //需要修改为并行化处理
        String[] ipSetsArray=ipSets.split("-");
        int num=ipSetsArray.length;//任务的数量
        int threadNum;//线程数量

        if(num<5){
            threadNum=num;
        }else{
            threadNum=taskNum;//一个用户的任务并发程度
        }

        final Table[] htableInterfaces=netflowInfoDAO.getHTableInterface(threadNum);//获取一个连接具柄
        ConcurrentLinkedQueue<Table> htableInterfaceQueues=new ConcurrentLinkedQueue<>(Arrays.asList(htableInterfaces));//维护一个连接池
        ExecutorService executorService=Executors.newFixedThreadPool(threadNum);//线程池

        for(final String ip:ipSetsArray){
            final long startTime=System.currentTimeMillis();
            final String[] IPAndMask=ip.split("%3A|:");
            if(IPAndMask.length!=2){
                //URL参数不对
                System.out.println("参数不对");
                break;
            }

            final Table table=htableInterfaceQueues.poll();//获取一个连接
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    final String[] IPAndMask=ip.split("%3A|:");
                    res.append(getAnalysisInfo(IPAndMask[0], Integer.parseInt(IPAndMask[1]), tableName, mode,table));//查询数据库并进行数据分析
                    res.append("*");
                    //写查询日志
                    long costTime=System.currentTimeMillis()-startTime;
                    if(mode.trim().equals("1")) {
                        try {
                            queryMode1Out.write("IP:" + IPAndMask[0] + "\t" + "TableName:" + tableName + "\t" + "CostTime:" + costTime + "\n");//写日志
                            queryMode1Out.flush();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }else if(mode.trim().equals("2")){
                        try {
                            queryMode2Out.write("IP:" + IPAndMask[0] + "\t" + "TableName:" + tableName + "\t" + "CostTime:" + costTime + "\n");//写日志
                            queryMode2Out.flush();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            htableInterfaceQueues.add(table);//返回连接具柄
        }

        executorService.shutdown();//关闭线程池
        while(!executorService.isTerminated()){//查询任务全部结束之后主闲扯功能继续向下执行
            //相当于线程池的join函数
        }

        htableInterfaceQueues=null;
        netflowInfoDAO.tablesClose(htableInterfaces);//关闭连接池
        return res;
    }

    /**
     * 根据HTTP请求参数查询相关地址的流记录并进行分析
     * 返回结果:根据模式的不同返回的数据类型不同
     * @return
     */
    public String getAnalysisInfo(String ip, int mask, String tableName, String mode, Table hTable){
        StringBuilder res=new StringBuilder();

        if(mode.trim().equals("1")){//chairs查询方式(查询并进行分析)
            List<NetflowInfo> srcNetflow=baseQueryInterface.queryBySrcIPSeg(ip,mask,tableName,hTable);
            List<List<String>> srcAnalysisResult=AnalysisCommunicationActivity.analysisResult(srcNetflow,ip,"srcIP");

            List<NetflowInfo> desNetflow=baseQueryInterface.queryByDesIPSeg(ip,mask,tableName,hTable);
            List<List<String>> desAnalysisResult=AnalysisCommunicationActivity.analysisResult(desNetflow,ip,"desIP");

            //传输的数据格式(暂时定成srcIP|DescIP的形式)
            res.append(AnalysisCommunicationActivity.mergeAnalysisInfo(srcAnalysisResult,"srcIP"));
            res.append("|");
            res.append(AnalysisCommunicationActivity.mergeAnalysisInfo(desAnalysisResult,"desIP"));
        }else if(mode.trim().equals("2")){//通用的查询方式返回原始的流记录
            List<NetflowInfo> srcNetflow=baseQueryInterface.queryBySrcIPSeg(ip,mask,tableName,hTable);
            List<NetflowInfo> desNetflow=baseQueryInterface.queryByDesIPSeg(ip,mask,tableName,hTable);
            res.append(NetflowInfo.mergeMutipliNetflow(srcNetflow));
            res.append("|\n");
            res.append(NetflowInfo.mergeMutipliNetflow(desNetflow));
        }
        return res.toString();
    }

    /**
     * 通过Post的方式发送HTTP请求
     */
    class PostQueryHandler implements HttpHandler {
        public void handle(HttpExchange httpExchange) throws IOException {
            InputStream in = httpExchange.getRequestBody();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            StringBuilder sb = new StringBuilder();//存储参数
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                sb.append(line).append('\n');
            }
            bufferedReader.close();

            Map<String,String> params=getParams(sb.toString());
            String ipSets=params.get("IpSets");
            String tableName=params.get("TableName");
            String mode =params.get("Mode");

            //执行请求
            OutputStream os = null;

            StringBuffer res=getInfoFromActivityDataBase(ipSets,tableName,mode);//从活动库查询信息

            httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, res.toString().getBytes().length);
            os = httpExchange.getResponseBody();
            os.write(res.toString().getBytes());

            os.flush();
            os.close();
            httpExchange.close();
        }
    }

    /**
     * 通过GET的方式获取请求方式
     */
    class GetQueryHandler implements HttpHandler {
        public void handle(HttpExchange httpExchange) throws IOException {
            String requestURI = httpExchange.getRequestURI().toString();//请求的URL
            Map<String,String> params=getParams(requestURI.split("\\?")[1]);

            String ipSets=params.get("IpSets");
            String tableName=params.get("TableName");
            String mode =params.get("Mode");

            //执行请求
            OutputStream os = httpExchange.getResponseBody();

            //循环执行
            StringBuffer res=getInfoFromActivityDataBase(ipSets,tableName,mode);

            httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, res.toString().getBytes().length);
            os.write(res.toString().getBytes());

            os.flush();
            os.close();
            httpExchange.close();
        }
    }
}