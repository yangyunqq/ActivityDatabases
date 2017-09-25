package queryIPCIS.chainLinkWithMap;

/**
 * java bean of address
 * 
 * @author jrhu05
 *
 */
public class Location_Bean {
	private String country;
	private String region;
	private String city;

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	

	public Location_Bean(String country, String region, String city) {
		this.country = country;
		this.region = region;
		this.city = city;
	}

	@Override
	public String toString() {
		return country + "-" + region + "-" + city;
	}

}
