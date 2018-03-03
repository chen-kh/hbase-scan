package act.nsfc.HbaseScan.Scan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;

public class HbaseScan extends Thread {

	private static final byte[] TABLE_NAME = Bytes.toBytes("trustcars");
	private static final byte[] OBDCF = Bytes.toBytes("O");
	private static final byte[] GPSCF = Bytes.toBytes("G");
	private static final byte[] RAWCF = Bytes.toBytes("R");// 列族

	private static Configuration conf;
	private static Map<String, byte[]> columns;
	public int num;
	public static Map<String, Long> amoutMap = new HashMap<String, Long>();
	public Map<String, Long> map = new HashMap<String, Long>();
	public boolean isfinish = false;

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

	public HbaseScan(int num) {
		this.num = num;
	}

	public static long scanGPS(long startTime, long stopTime, String devicesn) {
		JSONArray resultArray = new JSONArray();
		Integer count = 0;
		HTable table;
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
			// scanner.addColumn(GPSCF, columns.get("direction"));
			// scanner.addColumn(GPSCF, columns.get("speed"));

			scanner.setStartRow(startRow);
			scanner.setStopRow(stopRow);
			scanner.setReversed(true);

			ResultScanner resultScanner = table.getScanner(scanner);
			for (Result res : resultScanner) {
				res.size();
				if (res != null) {
					Double longitude = Bytes.toDouble(res.getValue(GPSCF, columns.get("longitude")));
					Double latitude = Bytes.toDouble(res.getValue(GPSCF, columns.get("latitude")));
					Integer direction = Bytes.toInt(res.getValue(GPSCF, columns.get("direction")));
					Double speed = Bytes.toDouble(res.getValue(GPSCF, columns.get("speed")));
					String rk = Bytes.toString(res.getRow());

					// get timestamp from rowkey
					long timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(rk.length() - 19, rk.length()));

					JSONObject object = new JSONObject();
					object.put("devicesn", devicesn);
					object.put("longitude", longitude);
					object.put("latitude", latitude);
					object.put("speed", speed);
					object.put("direction", direction);
					object.put("timestamp", timestamp);
					resultArray.put(object);
					count++;
				}
			}
			resultScanner.close();
			table.close();
			for (int i = 0; i < resultArray.length(); i++) {
				System.out.println(resultArray.getJSONObject(i));
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return count;
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
			scanner.setReversed(true);

			ResultScanner resultScanner = table.getScanner(scanner);
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
						resultArray.put(object);
						count++;
					}
					continue;
				}
			}
			resultScanner.close();
			table.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		for (int i = 0; i < resultArray.length(); i++) {
			System.out.println(resultArray.getJSONObject(i));
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
				// System.out.println(object);
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

	public void run() {
		// start timestamp
		long start = Utils.getTimeStamp("2016-06-01 00:00:00");
		// end timestamp
		long stop = Utils.getTimeStamp("2016-08-31 23:59:59");
		String devlistPath = "./devlist.txt";
		String[] devs = null;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(devlistPath)));
			String devlist = reader.readLine();
			devs = devlist.split(",");
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		for (int i = 0; i < devs.length; i++) {
			if (i % 10 == num) {
				String devicesn = devs[i];
				long count = 0;
				long s = System.currentTimeMillis();
				count = scanGPS(start, stop, devicesn);
				// count += scanOBD(start, stop, devicesn);
				map.put(devicesn, count);
				System.out.println(devicesn + ": " + count);
				long e = System.currentTimeMillis();
				System.out.println((e - s) / 1000.0);
			}
		}
		Iterator<Map.Entry<String, Long>> it = map.entrySet().iterator();
		synchronized (amoutMap) {
			while (it.hasNext()) {
				Map.Entry<String, Long> entry = it.next();
				amoutMap.put(entry.getKey(), entry.getValue());
			}
		}
		isfinish = true;
		// File file = new
		// File("E:\\eclipse\\project\\HbaseScanClient\\carlist.txt");
		// try {
		// BufferedReader br = new BufferedReader(new FileReader(file));
		// String line = "";
		// ArrayList<String> devicesnlist = new ArrayList<String>();
		// while ((line = br.readLine()) != null) {
		// devicesnlist.add(line);
		// }
		// br.close();
		// for (int t = 0; t < devicesnlist.size(); t ++) {
		// String devicesn = devicesnlist.get(t);
		// count += scanGPS(start, stop, devicesn);
		// count += scanOBD(start, stop, devicesn);
		// System.out.println(devicesn + ": " + count);
		// }
		//
		// }catch(Exception e) {
		// System.out.println(e.getMessage());
		// }
	}

	public static void main(String[] args) {
		List<HbaseScan> scannerList = new ArrayList<HbaseScan>();
		for (int i = 0; i < 15; i++) {
			HbaseScan scnner = new HbaseScan(i);
			scannerList.add(scnner);
			scnner.start();
		}
		int n = 0;
		while (n != 15) {
			n = 0;
			for (int i = 0; i < 15; i++) {
				if (!scannerList.get(i).isfinish) {
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					break;
				}
				n++;
			}
		}

		FileWriter writer = null;
		try {
			writer = new FileWriter(new File("./count.txt"));
			Iterator<Map.Entry<String, Long>> it = HbaseScan.amoutMap.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Long> entry = it.next();
				writer.write(entry.getKey() + ":" + entry.getValue());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
