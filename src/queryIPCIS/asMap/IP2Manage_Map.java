package queryIPCIS.asMap;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import queryIPCIS.chainLinkWithMap.IP2Lcotion_Map;
import queryIPCIS.mysql.ConnectMysql;

/**
 * IPv4管理归属库内存表CRUD的具体实现
 */
public class IP2Manage_Map {

	// 存储容器
	public static Map<Integer, ArrayList<AS_Bean>> IP2ManageMap = null;//管理归属库

	/**
	 * 读取指定路径的表到内存
	 */
	public static void readTable() throws SQLException {
		System.out.println("开始读取管理归属库...");
		//读取数据库
		long starTime = System.currentTimeMillis();

		Connection conn= ConnectMysql.getConnection();
		Statement statement = conn.createStatement();
		String sql="select prefix,mask,country,isp,unit from IP2Manage";
		ResultSet res=statement.executeQuery(sql);

		Map<Integer, ArrayList<AS_Bean>> IPSTable = new HashMap<Integer, ArrayList<AS_Bean>>();//存放整个使用位置库
		while(res.next()){
			long prefix=res.getLong("prefix");
			long mask=res.getLong("mask");
			String country=res.getString("country");
			String isp=res.getString("isp");
			String unit=res.getString("unit");

			long ipEnd=prefix+(long) Math.pow(2,32-mask)-1;

			AS_Bean targetAS_Bean = new AS_Bean(prefix, ipEnd, country,isp,unit);


			int hashCode = IP2Lcotion_Map.getID(prefix);//计算hashCode

			if (IPSTable.containsKey(hashCode)) {
				// 有这个点，直接取出list来添加
				List<AS_Bean> targetLink = IPSTable.get(hashCode);
				targetLink.add(targetAS_Bean);
			} else {
				List<AS_Bean> targetLink = new ArrayList<AS_Bean>();
				targetLink.add(targetAS_Bean);
				IPSTable.put(hashCode, (ArrayList<AS_Bean>) targetLink);
			}
		}
		IP2ManageMap=IPSTable;
		long endTime = System.currentTimeMillis();
		System.out.println("读入管理归属库耗时:"+ (endTime - starTime)/1000+ "ms");
	}

	/**
	 * 在内存表中查找指定IP对应的位置信息
	 * 
	 * @param IP
	 *            待查IP
	 * @return IPS_Bean
	 */
	public static AS_Bean searchTable(Long IP) {

		AS_Bean nullAS = new AS_Bean(0, 0, "","","");//查询不到结果

		int hashCode = IP2Lcotion_Map.getID(IP);

		while (hashCode >= 0) {
			//存在那个key值链
			if (IP2ManageMap.containsKey(hashCode)) {
				ArrayList<AS_Bean> targetList = IP2ManageMap.get(hashCode);
				for (AS_Bean ips_Bean : targetList) {
					long targetIPstart = ips_Bean.getStartIP();
					long targetIPend = ips_Bean.getEndIP();
					if (targetIPend >= IP && targetIPstart <= IP) {
						return ips_Bean;
					} else if (hashCode > targetIPstart) {
						//超过了可能的范围
						return nullAS;
					}
				}
			}
			hashCode--;
		}
		return nullAS;
	}
}
