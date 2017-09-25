package queryIPCIS.asMap;

/**
 * ISP信息基本javabean
 * 
 * @author jrhu05
 *
 */
public class AS_Bean {
	private long startIP;// 开始IP
	private long endIP;// 结束ip
	private String country;
	private String isp;
	private String unit;

	/**
	 * 获得开始IP
	 * @return 开始IP
	 */
	public long getStartIP() {
		return startIP;
	}

	/**
	 * 设置开始IP
	 *
	 * @param startIP
	 *            新的开始IP值
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
	 * 重设结束IP
	 *
	 * @param endIP
	 *            新的结束IP
	 */
	public void setEndIP(long endIP) {
		this.endIP = endIP;
	}

	/**
	 * 带参数的构造函数
	 */
	public AS_Bean(long startIP, long endIP, String country,String isp,String unit) {
		this.startIP = startIP;
		this.endIP = endIP;
		this.country = country;
		this.isp=isp;
		this.unit=unit;
	}

	/**
	 * 重写toString方法
	 */
	@Override
	public String toString() {
		return country+"-"+isp+"-"+unit;
	}

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
		AS_Bean other = (AS_Bean) obj;
		if (startIP != other.startIP)
			return false;
		return true;
	}

}
