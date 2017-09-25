package queryIPCIS.chainLinkWithMap;

import activityQuery.CommonTools;
import queryIPCIS.mysql.ConnectMysql;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;


/**
 * IPv4地理位置库内存表CRUD实现
 * 
 * @author jrhu05
 *
 */
public class IP2Lcotion_Map {
	private static Map<Integer, ArrayList<IPS_Bean>> IP2LocationtMap = null;//使用位置数据库

	/**
	 * 读表
	 * @return IPSTable 读入的内存表
	 */
	public static void readTable() throws SQLException {
		System.out.println("开始读取使用位置库...");
		//读取数据库
		long starTime = System.currentTimeMillis();

		Connection conn= ConnectMysql.getConnection();
		Statement statement = conn.createStatement();
		String sql="select ipStart,ipEnd,country,region,city from IP2Location";
		ResultSet res=statement.executeQuery(sql);

		Map<Integer, ArrayList<IPS_Bean>> IPSTable = new HashMap<Integer, ArrayList<IPS_Bean>>();//存放整个使用位置库
		while(res.next()){
			long ipStart=res.getLong("ipStart");
			long ipEnd=res.getLong("ipEnd");
			String country=res.getString("country");
			String region=res.getString("region");
			String city=res.getString("city");

			Location_Bean location = new Location_Bean(country, region, city);

			IPS_Bean targetIPS_Bean = new IPS_Bean(ipStart, ipEnd, location);

			int hashCode = getID(ipStart);//计算hashCode

			if (IPSTable.containsKey(hashCode)) {
				List<IPS_Bean> targetLink = IPSTable.get(hashCode);
				targetLink.add(targetIPS_Bean);
			} else {
				List<IPS_Bean> targetLink = new ArrayList<IPS_Bean>();
				targetLink.add(targetIPS_Bean);
				IPSTable.put(hashCode, (ArrayList<IPS_Bean>) targetLink);
			}
		}
		IP2LocationtMap=IPSTable;
		long endTime = System.currentTimeMillis();
		System.out.println("读入使用位置库耗时:"+ (endTime - starTime)/1000+ "ms");
	}
	/**
	 * 打印表
	 */
	@SuppressWarnings({ "rawtypes", "unchecked", "unused" })
	public static void showTable() {
		// hashmap遍历
		Iterator iter = IP2LocationtMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			Integer key = (Integer) entry.getKey();
			ArrayList<IPS_Bean> targetList = (ArrayList<IPS_Bean>) entry.getValue();
			// 每个targetlist输出
			for (IPS_Bean targetBean : targetList) {
				System.out.print(targetBean.toString());
			}
		}
	}

	/**
	 * 使用高26位生成hashcode
	 * 
	 * //@param startIP
	 *            起始IP
	 * @return hasCode hashcode
	 **/
	public static int getID(Long startIP) {
		String starIPBinaryString = Long.toBinaryString(startIP);
		//补0
		StringBuilder stringBuilder=new StringBuilder(starIPBinaryString);
		for(int i=0;i<32-starIPBinaryString.length();i++){
			stringBuilder.insert(0,"0");
		}
		starIPBinaryString=stringBuilder.substring(0, 16);
		return Integer.valueOf(starIPBinaryString,2);
	}

	/*测试的main函数
	public static void main(String[] args){
		try {
			readTable();
			Scanner in=new Scanner(System.in);

			while(in.hasNext()){
				long startTIme=System.currentTimeMillis();
				long IP= CommonTools.getIPNum(in.nextLine());
				for(int i=0;i<100000;i++) {
					System.out.println(searchTable(IP));
				}
				System.out.println(System.currentTimeMillis()-startTIme);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}*/

	/**
	 * 搜索
	 * @param IP 待搜索IP
	 * @return result 搜索结果IPS_Bean
	 **/
	public static IPS_Bean searchTable(Long IP) {

		Location_Bean nullLocation = new Location_Bean("", "", "");

		IPS_Bean nullIPS_Bean = new IPS_Bean(0, 0, nullLocation);

		int hashCode = getID(IP);

		while (hashCode >= 0) {
			//存在那个key值链
			if (IP2LocationtMap.containsKey(hashCode)) {
				ArrayList<IPS_Bean> targetList = IP2LocationtMap.get(hashCode);
				for (IPS_Bean ips_Bean : targetList) {
					long targetIPstart = ips_Bean.getStartIP();
					long targetIPend = ips_Bean.getEndIP();
					if (targetIPend >= IP && targetIPstart <= IP) {
						return ips_Bean;
					} else if (hashCode > targetIPstart) {
						//超过了可能的范围
						return nullIPS_Bean;
					}
				}
			}
			hashCode--;
		}

		return nullIPS_Bean;
	}
}
