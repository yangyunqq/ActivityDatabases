package queryIPCIS.chainLinkWithMap;

/**
 * IPS java bean，对应数据库里的一条记录
 * 
 * @author jrhu05
 *
 */

public class IPS_Bean {
	private long startIP;// 起始IP
	private long endIP;// 结束IP
	private Location_Bean location;// 位置
	/**
	 * 获得起始IP
	 * 
	 * @return 起始IP
	 */
	public long getStartIP() {
		return startIP;
	}

	/**
	 * 设置起始IP
	 * 
	 * @param startIP
	 *            新的起始IP
	 */
	public void setStartIP(long startIP) {
		this.startIP = startIP;
	}

	/**
	 * 获得结束IP
	 * 
	 * @return 结束IP
	 */
	public long getEndIP() {
		return endIP;
	}

	/**
	 * 设置结束IP
	 *            新的结束IP
	 */
	public void setEndIP(long endIP) {
		this.endIP = endIP;
	}

	/**
	 * 获得某个ips bean的位置信息
	 * 
	 * @return 位置信息
	 */
	public Location_Bean getLocation() {
		return location;
	}

	/**
	 * 设置某个ips bean的位置信息
	 * 
	 * @return 新的位置信息
	 */
	public void setLocation(Location_Bean location) {
		this.location = location;
	}
	
	

	/**
	 * 空构造
	 */
	public IPS_Bean() {
		super();
	}

	/**
	 * 重写toString方法
	 */
	@Override
	public String toString() {
		return location.toString();
	}
 
	 
	/**
	 * 带参构造
	 * 
	 * @param startIP
	 *            起始IP
	 * @param endIP
	 *            结束IP
	 * @param location
	 *            位置信息
	 */
	public IPS_Bean(long startIP, long endIP, Location_Bean location) {
		super();
		this.startIP = startIP;
		this.endIP = endIP;
		this.location = location;
	}

	// rewrite hashcode and equals to do judge two records equals or not due to
	// whether the startIP is
	/**
	 * 重写hashcode方法
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (startIP ^ (startIP >>> 32));
		return result;
	}


	/**
	 * 重写equals方法
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IPS_Bean other = (IPS_Bean) obj;
		if (startIP != other.startIP)
			return false;
		return true;
	}


}
