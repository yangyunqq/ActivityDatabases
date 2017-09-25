package activityQuery;

import com.sun.net.httpserver.HttpServer;
import queryIPCIS.asMap.IP2Manage_Map;
import queryIPCIS.chainLinkWithMap.IP2Lcotion_Map;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * IP流量活动数据库查询服务器
 * Created by yangyun on 2017/5/12.
 */
public class QueryServer extends Thread{
    private int threadNum=20;
    private ExecutorService executorService= Executors.newFixedThreadPool(threadNum);

    public void run(){
        String serverIP = "211.65.197.210";//服务器地址
        int port=8080;//端口
        try {
            HttpServer httpServer=HttpServer.create(new InetSocketAddress(serverIP,port),10);
            httpServer.createContext("/IPCIS/activityDatabase",new QueryHttphandler());//设置如何处理这个请求
            httpServer.setExecutor(executorService);//设置线程池

            //缓存管理归属库数据表
            try {
                IP2Manage_Map.readTable();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            //缓存使用位置库数据表
            try {
                IP2Lcotion_Map.readTable();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            System.out.println("服务器已经开启");
            httpServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        new QueryServer().start();
    }
}