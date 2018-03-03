package act.nsfc.HbaseScan.Scan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;;

public class HbaseScanAll extends Thread {

	private static final byte[] TABLE_NAME = Bytes.toBytes("trustcars");
	private static final byte[] OBDCF = Bytes.toBytes("O");
	private static final byte[] GPSCF = Bytes.toBytes("G");
	private static final byte[] RAWCF = Bytes.toBytes("R");// ����
	private int tag;
	private static Configuration conf;
	private static Map<String, byte[]> columns;

	public HbaseScanAll(int tag) {
		this.tag = tag;
	}

	static {
		Configuration customConf = new Configuration();
		customConf.setStrings("hbase.zookeeper.quorum", "192.168.10.101");
		customConf.setLong("hbase.rpc.timeout", 600000);
		customConf.setLong("hbase.client.scanner.caching", 1000);
		customConf.set("hbase.zookeeper.property.clientPort", "2181");
		conf = HBaseConfiguration.create(customConf);

		columns = new HashMap<String, byte[]>();
		columns.put("longitude", Bytes.toBytes("JD"));
		columns.put("latitude", Bytes.toBytes("WD"));
		columns.put("direction", Bytes.toBytes("D"));
		columns.put("speed", Bytes.toBytes("S"));

		columns.put("totalfuel", Bytes.toBytes("TF"));
		columns.put("totalmileage", Bytes.toBytes("TM"));
		columns.put("mileage", Bytes.toBytes("M"));
		columns.put("OBDspeed", Bytes.toBytes("S"));
		columns.put("enginespeed", Bytes.toBytes("ES"));

	}

	public static int scanGPS(long startTime, long stopTime, String devicesn) {
		JSONArray resultArray = new JSONArray();
		Integer count = 0;
		HTable table;
		String gpsfilename = "D:/11data/gps_910/" + devicesn + "gps.txt";
		FileWriter fws = null;
		try {
			fws = new FileWriter(new File(gpsfilename));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {
			table = new HTable(conf, TABLE_NAME);

			byte[] startRow = Utils.generateRowkeyPM(startTime, devicesn);
			byte[] stopRow = Utils.generateRowkeyPM(stopTime, devicesn);

			Scan scanner = new Scan();
			// scanner.setFilter(new PageFilter(1));
			scanner.setCaching(100);
			scanner.addFamily(GPSCF);
			scanner.addColumn(GPSCF, columns.get("longitude"));
			scanner.addColumn(GPSCF, columns.get("latitude"));
			scanner.addColumn(GPSCF, columns.get("direction"));
			scanner.addColumn(GPSCF, columns.get("speed"));

			scanner.setStartRow(startRow);
			scanner.setStopRow(stopRow);
			scanner.setReversed(true);

			ResultScanner resultScanner = table.getScanner(scanner);
			for (Result res : resultScanner) {
				if (res != null) {
					Double longitude = Bytes.toDouble(res.getValue(GPSCF, columns.get("longitude")));
					Double latitude = Bytes.toDouble(res.getValue(GPSCF, columns.get("latitude")));
					Integer direction = Bytes.toInt(res.getValue(GPSCF, columns.get("direction")));
					Double speed = Bytes.toDouble(res.getValue(GPSCF, columns.get("speed")));
					String rk = Bytes.toString(res.getRow());

					long timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(rk.length() - 19, rk.length()));

					JSONObject object = new JSONObject();
					object.put("devicesn", devicesn);
					object.put("longitude", longitude);
					object.put("latitude", latitude);
					object.put("speed", speed);
					object.put("direction", direction);
					object.put("timestamp", timestamp);
					System.out.println(object.toString());
					fws.write(object.toString() + "\n");
					count++;
				}
			}
			resultScanner.close();
			table.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return count;
		} finally {
			try {
				fws.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return count;
	}

	public static Integer scanOBD(long startTime, long stopTime, String devicesn) {
		JSONArray resultArray = new JSONArray();
		HTable table;
		Integer count = 0;
		try {
			table = new HTable(conf, TABLE_NAME);
			byte[] startRow = Utils.generateRowkeyPM(startTime, devicesn);
			byte[] stopRow = Utils.generateRowkeyPM(stopTime, devicesn);

			Scan scanner = new Scan();
			scanner.addFamily(OBDCF);
			scanner.addColumn(OBDCF, columns.get("totalfuel"));
			scanner.addColumn(OBDCF, columns.get("totalmileage"));
			scanner.addColumn(OBDCF, columns.get("mileage"));
			scanner.addColumn(OBDCF, columns.get("OBDspeed"));
			scanner.addColumn(OBDCF, columns.get("enginespeed"));

			// scanner.setStartRow(startRow);
			// scanner.setStopRow(stopRow);
			// scanner.setReversed(true);
			scanner.setStartRow(stopRow);
			scanner.setStopRow(startRow);
			// scanner.setReversed(true);

			ResultScanner resultScanner = table.getScanner(scanner);
			long num = 0L;
			for (Result res : resultScanner) {
				if (res != null) {
					String rk = Bytes.toString(res.getRow());
					// get timestamp form rowkey
					long timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(rk.length() - 19, rk.length()));

					Double totalfuel = Bytes.toDouble(res.getValue(OBDCF, columns.get("totalfuel")));
					Double totalmileage = Bytes.toDouble(res.getValue(OBDCF, columns.get("totalmileage")));
					Double mileage = Bytes.toDouble(res.getValue(OBDCF, columns.get("mileage")));
					Double OBDspeed = Bytes.toDouble(res.getValue(OBDCF, columns.get("OBDspeed")));
					Double enginespeed = Bytes.toDouble(res.getValue(OBDCF, columns.get("enginespeed")));
					if (totalmileage > 0) {

						JSONObject object = new JSONObject();
						object.put("devicesn", devicesn);
						object.put("totalfuel", totalfuel);
						object.put("totalmileage", totalmileage);
						object.put("mileage", mileage);
						object.put("OBDspeed", OBDspeed);
						object.put("enginespeed", enginespeed);
						object.put("timestamp", timestamp);
						System.out.println(object.toString());
						resultArray.put(object);
						count++;
					}
					continue;
				}
			}
			resultScanner.close();
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}

		return count;

	}

	public static JSONArray scanRawOBD(long startTime, long stopTime, String devicesn) throws Exception {
		JSONArray resultArray = new JSONArray();
		HTable table = new HTable(conf, TABLE_NAME);

		byte[] startRow = Utils.generateRowkeyPM(startTime, devicesn);
		byte[] stopRow = Utils.generateRowkeyPM(stopTime, devicesn);

		Scan scanner = new Scan();
		scanner.addFamily(RAWCF);

		scanner.setStartRow(stopRow);
		scanner.setStopRow(startRow);
		// scanner.setReversed(true);

		ResultScanner resultScanner = table.getScanner(scanner);
		for (Result res : resultScanner) {
			if (res != null) {
				// String rk = Bytes.toString(res.getRow());
				// get timestamp from rowkey
				// long timestamp = Long.MAX_VALUE
				// - Long.valueOf(rk.substring(rk.length() - 19,
				// rk.length()));
				
				JSONObject object = new JSONObject(Bytes.toString(res.getValue(RAWCF, RAWCF)));
				if(object.has("1")){
					System.out.println("has 1");
					System.out.println(object);	
				}
				int sum = 0;
				if (object.has("8")) {
					sum += object.getInt("8");
					if (object.getInt("8") != 0)
						// System.out.println(object.getInt("8"));
						;
				}
				if (object.has("9")) {
					sum += object.getInt("9");
					if (object.getInt("9") != 0)
						// System.out.println(object.getInt("9"));
						;

				}
				if (object.has("10")) {
					sum += object.getInt("10");
					if (object.getInt("10") != 0)
						// System.out.println(object.getInt("10"));
						;

				}
				if (object.has("11")) {
					sum += object.getInt("11");
					if (object.getInt("11") != 0)
						// System.out.println(object.getInt("11"));
						;

				}
				if (sum != 0)
					// System.out.println(sum);
					;
				if (object.has("65") && object.getDouble("65") > 0) {
					resultArray.put(object);
				}
			}

		}
		resultScanner.close();
		table.close();

		return resultArray;

	}

	public static void SaveToFile(JSONArray resultArray, String dsn, int i) throws IOException {
		FileWriter fws;
		String ra = resultArray.toString();
		String obdfilename = "D:/11data/gps_910/" + dsn + "obd.txt";
		// + data + ".txt";
		String gpsfilename = "D:/11data/gps_910/" + dsn + "gps.txt";
		// + data + ".txt";
		String eventsfilename = "D:/11data/gps_910/" + dsn + "events.txt";
		// + data + ".txt";

		File file = null;
		String s[] = ra.split("},");
		if (i == 1) {
			file = new File(obdfilename);
		} else if (i == 2) {
			file = new File(gpsfilename);
		} else if (i == 3) {
			file = new File(eventsfilename);
		}

		fws = new FileWriter(file);
		// int slen = s.length;
		// int k = 0;
		// System.out.println(k);
		// while (k < slen) {
		// fws.write(s[k] + "}");
		// k++;
		// fws.write("\n");
		// }
		Iterator<Object> it = resultArray.iterator();
		while (it.hasNext()) {
			JSONObject jo = (JSONObject) it.next();
			fws.write(jo.toString() + "\n");
		}
		fws.close();
	}

	// public static void SaveToFile2(JSONArray resultArray, String dsn, int i)
	// throws IOException {
	// FileWriter fws;
	// String ra = resultArray.toString();
	// String obdfilename = "./sun_gps/" + dsn + "obd.txt";
	// // + data + ".txt";
	// String gpsfilename = "./sun_gps/" + dsn + "gps.txt";
	// // + data + ".txt";
	// String eventsfilename = "./sun_gps/" + dsn + "events.txt";
	// // + data + ".txt";
	//
	// File file = null;
	// String s[] = ra.split("},");
	// if (i == 1) {
	// file = new File(obdfilename);
	// } else if (i == 2) {
	// file = new File(gpsfilename);
	// } else if (i == 3) {
	// file = new File(eventsfilename);
	// }
	//
	// fws = new FileWriter(file);
	// int k = s.length - 1;
	// System.out.println(k);
	// while (k >= 0) {
	// fws.write(s[k] + "}");
	// k--;
	// fws.write("\n");
	// }
	// fws.close();
	// }

	public void run() {
		long start = Utils.getTimeStamp("2016-07-01 00:00:00");
		long stop = Utils.getTimeStamp("2016-12-31 00:00:00");
		File file = new File("D:/11data/BJ_all_devicesn.txt");
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = "";
			ArrayList<String> devicesnlist = new ArrayList<String>();
			while ((line = br.readLine()) != null) {
				devicesnlist.add(line);
			}
			br.close();
			for (int t = 0; t < devicesnlist.size(); t++) {
				if (t % 10 == tag) {
					String devicesn = devicesnlist.get(t);
					int count = scanGPS(start, stop, devicesn);
					if (count == 0) {
						System.out.println("1 - no data for devicesn = " + devicesn);
					} else {
						System.out.println("save data of devicesn = " + devicesn);
					}
				}
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	public static void main(String[] args) {
//		for (int i = 0; i < 1; i++) {
//			new HbaseScanAll(i).start();
//		}
		long start = Utils.getTimeStamp("2016-01-01 05:03:17");
		long stop = Utils.getTimeStamp("2016-01-01 05:21:21");
		String devicesn = "967790028738";
		try {
			HbaseScanAll.scanGPS(start, stop, devicesn);
		} catch (Exception e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
	}
}
