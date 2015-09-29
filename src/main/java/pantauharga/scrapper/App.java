package pantauharga.scrapper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Cleaner;
import org.jsoup.safety.Whitelist;
import org.jsoup.select.Elements;

public class App {
	private Connection connection;
	private Properties configProperties;

	public App(String propFileName) throws Exception {
		this.loadConfigProperties(propFileName);
		this.connectToDatabase();
	}

	private void loadConfigProperties(String propFileName) throws Exception {
		if (this.configProperties == null) {
			this.configProperties = new Properties();
			InputStream inputStream = null;
			try {
				//String propFileName = "App.properties";
				inputStream = new FileInputStream(propFileName);
				this.configProperties.load(inputStream);
			} catch (Exception e) {
				throw e;
			} finally {
				if (inputStream != null)
					inputStream.close();
			}
		}
	}

	private void connectToDatabase() throws Exception {
		String driver = this.configProperties.getProperty("dataSource.driverClassName");
		String url = this.configProperties.getProperty("dataSource.url");
		String user = this.configProperties.getProperty("dataSource.username");
		String pass = this.configProperties.getProperty("dataSource.password");
		Class.forName(driver);
		System.out.println("Connecting to database...");
		this.connection = DriverManager.getConnection(url, user, pass);
	}

	private Integer getUserId(String username) throws Exception {
		Statement stmt = null;
		ResultSet rs = null;
		Integer userId = null;
		try {
			stmt = this.connection.createStatement();
			String sql = String.format("select id from auth_user where username = '%s'", username);
			rs = stmt.executeQuery(sql);
			if (rs.next()) {
				userId = rs.getInt("id");
				//System.out.println(String.format("Got user id for %s: %d", username, userId));
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null)
				rs.close();
			if (stmt != null)
				stmt.close();
		}
		return userId;
	}

	private HashMap<String, Geolocation> getGeolocationByName(List<CommodityInput> inputList) throws Exception {
		HashMap<String, Geolocation> locationMap = new HashMap<String, Geolocation>();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = this.connection.prepareStatement("select id, name, geolocation from region where upper(name) = ?");
			for (CommodityInput input : inputList) {
				String upcased = input.location.toUpperCase();
				Geolocation location = locationMap.get(upcased);
				if (location == null) {
					pstmt.setString(1, upcased);
					rs = pstmt.executeQuery();
					if (rs.next()) {
						String[] locationArr = rs.getString("geolocation").split(",");
						double lat = Double.parseDouble(locationArr[0]);
						double lng = Double.parseDouble(locationArr[1]);
						locationMap.put(rs.getString("name").toUpperCase(), new Geolocation(lat, lng, rs.getInt("id")));
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
		}
		return locationMap;
	}

	private HashMap<String, Integer> getComodityMap(List<CommodityInput> inputList) throws Exception {
		HashMap<String, Integer> commodityMap = new HashMap<String, Integer>();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = this.connection.prepareStatement("select id, name from comodity where upper(name) = ?");
			for (CommodityInput input : inputList) {
				String upcased = input.name.toUpperCase();
				Integer id = commodityMap.get(upcased);
				if (id == null) {
					pstmt.setString(1, upcased);
					rs = pstmt.executeQuery();
					if (rs.next()) {
						commodityMap.put(rs.getString("name").toUpperCase(), rs.getInt("id"));
					}
				}
			}			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
		}
		return commodityMap;
	}

	
	public void batchInsertCommodityInput(List<CommodityInput> inputList) throws Exception {
		Integer userId = this.getUserId("admin");
		HashMap<String, Geolocation> locationMap = this.getGeolocationByName(inputList);
		HashMap<String, Integer> comodityMap = this.getComodityMap(inputList);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Date now = new Date();
		try {
			pstmt = this.connection.prepareStatement("insert into comodity_input values("
					+ "(select max(id)+1 from comodity_input),"
					+ "0,"
					+ "0,"
					+ "?," // comodity id
					+ "?," // date created
					+ "0,"
					+ "?," // last updated
					+ "?," // lat
					+ "?," // lng
					+ "?," // price
					+ "?," // region id
					+ "?" // user id
					+ ")");
			for (CommodityInput input : inputList) {
				Integer commodityId = comodityMap.get(input.name.toUpperCase());
				Geolocation location = locationMap.get(input.location.toUpperCase());
				if (commodityId != null && location != null) {					
					pstmt.setInt(1, commodityId);
					pstmt.setDate(2, new java.sql.Date(now.getTime()));
					pstmt.setDate(3, new java.sql.Date(now.getTime()));
					pstmt.setDouble(4, location.lat);
					pstmt.setDouble(5, location.lng);
					pstmt.setInt(6, new Double(input.price).intValue());
					pstmt.setInt(7, location.id);
					pstmt.setInt(8, userId);
					try {
						System.out.println(pstmt.toString());
						pstmt.execute();						
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null)
				rs.close();
			if (pstmt != null)
				pstmt.close();
		}
		
	}
 	
	public void close() throws Exception {
		if (this.connection != null) {
			this.connection.close();
		}
	}

	public static void main(String[] args) {

		try {
			App scrapper = new App(args[0]);
			String url = String.format("http://aplikasi.pertanian.go.id/smshargakab/%1$s.asp?selrepo=%1$s&seltgl=%2$td&selbul=%2$tm&seltah=%2$ty", args[1], Calendar.getInstance());
			System.out.println(url);
			System.out.println("Start scrapping..");
			Document doc = Jsoup.connect(url).get();
			if (doc != null) {
				// remove unnecessary tags and attributes
				Whitelist whitelist = Whitelist.relaxed().removeTags("b", "font").removeAttributes("td", "width")
						.removeAttributes("table", "width");
				Cleaner cleaner = new Cleaner(whitelist);
				Document cleanDoc = cleaner.clean(doc);

				// get commodity name, price and location
				Elements trs = cleanDoc.select("table:nth-child(2) tr");
				if (trs != null && trs.first() != null) {
					Elements firstTds = trs.first().children();
					List<CommodityInput> inputList = new ArrayList<CommodityInput>();
					for (int i = 2; i < firstTds.size(); i++) {
						for (int j = 2; j < trs.size(); j++) {
							Element tr = trs.get(j);
							try {
								String name = firstTds.get(i).text().trim().replaceAll("\\s*\\(Rp/\\w+\\)\\s*", "").replaceAll("^[^\\w]+|[^\\w]+$", "");
								String location = tr.child(1).text().trim().replaceAll("^[^\\w]+|[^\\w]+$", "");
								double price = Double.parseDouble(tr.child(i).text().replaceAll(",", ""));
								// exclude if price <= 0
								if (price > 0) {
									inputList.add(new CommodityInput(name, location, price));
								}
							} catch (Exception ex) {
								// continue
								System.err.println(ex.getMessage());
							}
						}
					}
					System.out.println("Done scrapping.");

					// insert like a boss
					System.out.println("Inserting to database..");
					scrapper.batchInsertCommodityInput(inputList);
					System.out.println("Done inserting.");

				} else {
					System.err.println("Data is not yet available");
				}
			}

			scrapper.close();
			System.out.println("Bye.");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

class CommodityInput {
	String name;
	String location;
	double price;

	public CommodityInput(String name, String location, double price) {
		this.name = name;
		this.location = location;
		this.price = price;
	}

	@Override
	public String toString() {
		return String.format("%s dijual seharga Rp %.2f/kg di %s", this.name, this.price, this.location);
	}
}

class Geolocation {
	public int id;
	public double lat;
	public double lng;

	public Geolocation(double lat, double lng, int id) {
		this.lat = lat;
		this.lng = lng;
		this.id = id;
	}

	@Override
	public String toString() {
		return String.format("(%s, %s)", this.lat, this.lng);
	}
}