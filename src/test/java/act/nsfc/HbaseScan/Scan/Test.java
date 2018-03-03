package act.nsfc.HbaseScan.Scan;

import java.sql.Timestamp;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class Test extends Thread {
	public static void main(String[] args) throws InterruptedException {
		netJsonArrayTest();
		orgJsonArrayTest();
		testRowKey();
		System.out.println((Long.MAX_VALUE + "").length());
	}

	private static void testRowKey() throws InterruptedException {
		long timeNow1 = System.currentTimeMillis();
		sleep(1 * 1000);
		long timeNow2 = System.currentTimeMillis();
		System.out.println(timeNow1);
		System.out.println(timeNow2);
		System.out.println(new String(Utils.generateRowkeyPM(timeNow1, "967790139049")));
		System.out.println(new String(Utils.generateRowkeyPM(timeNow2, "967790139049")));
	}

	private static void orgJsonArrayTest() {
		org.json.JSONArray resultArray = new org.json.JSONArray();
		JSONObject object = new JSONObject();
		object.put("devicesn", "1");
		// object.put("longitude", 123.45);
		// object.put("latitude", 234.56);
		// object.put("speed", 56);
		// object.put("direction", 90);
		// object.put("timestamp", 123456789);
		resultArray.put(object);
		object.put("devicesn", "2");
		// object.put("longitude", 123.45);
		// object.put("latitude", 234.56);
		// object.put("speed", 56);
		// object.put("direction", 90);
		// object.put("timestamp", 123456789);
		resultArray.put(object);
		object.put("devicesn", "3");
		// object.put("longitude", 123.45);
		// object.put("latitude", 234.56);
		// object.put("speed", 56);
		// object.put("direction", 90);
		// object.put("timestamp", 123456789);
		resultArray.put(object);
		System.out.println("resultArrray ：" + resultArray.toString());
		for (int i = 0; i < resultArray.length(); i++) {
			System.out.println(resultArray.get(i));
		}
	}

	public static void netJsonArrayTest() {
		JSONArray jsonArray = new JSONArray();
		jsonArray.add(JSONObject.fromObject("{\"a\":23}"));
		jsonArray.add(JSONObject.fromObject("{\"a\":24}"));
		jsonArray.add(JSONObject.fromObject("{\"a\":25}"));
		jsonArray.add(JSONObject.fromObject("{\"a\":26}"));
		System.out.println(jsonArray.toString());
		for (int i = 0; i < jsonArray.size(); i++) {
			System.out.println(jsonArray.getJSONObject(i));
		}
	}

	public static void timestampTest() {
		Timestamp timestamp = new Timestamp(0L);
		System.out.println(timestamp);
		timestamp = new Timestamp(6 * 100L);
		System.out.println(timestamp);
		timestamp = new Timestamp(System.currentTimeMillis());
		System.out.println(timestamp);
		timestamp = Timestamp.valueOf("2017-01-17 14:44:30.535");
		System.out.println(timestamp);
		System.out.println(timestamp.getTime());
		timestamp = Timestamp.valueOf("2017-01-17 14:44:31.535");
		System.out.println(timestamp);
		System.out.println(timestamp.getTime());
	}
}
