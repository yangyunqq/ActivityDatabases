package queryIPCIS.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by yangyun on 2017/1/5.
 */
public class ConnectMysql {
    public static Connection getConnection(){
        // 驱动程序名
        String driver = "com.mysql.jdbc.Driver";
        // URL指向要访问的数据库名world
        String url = "jdbc:mysql://211.65.193.23:3306/center_ipcis";
        // MySQL配置时的用户名
        String user = "root";
        // MySQL配置时的密码
        String password = "admin246531";
        Connection conn=null;
        try {
            Class.forName(driver);
            try {
                conn = DriverManager.getConnection(url, user, password);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return conn;
    }
}
