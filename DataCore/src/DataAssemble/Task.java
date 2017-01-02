package DataAssemble;

import net.spy.memcached.MemcachedClient;
import util.Calculator;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by Quanyang Liu on 10/18/16.
 */
public class Task {
	public List<DataAccess> dataAccess;
	public DataAssemble dataAssemble;

	public Task() {
		dataAccess = new ArrayList<DataAccess>();
	}

	public static Task FromDocument(BsonDocument document, MemcachedClient memcachedClient) {
		// System.out.println(document.toJson());
		Task task = new Task();

		// System.out.println("aaaa");
		int frequency = document.getInt32("frequency").getValue();
		String resultDataName = document.getString("name").getValue();
		String type = document.getString("type").getValue();
		// System.out.println(frequency);
		// System.out.println(resultDataName);

		List<String> sourceDataIds = new ArrayList<>();
		// System.out.println(document.getArray("dataSourceList").toString());
		for (Object source : document.getArray("dataSourceList")) {
			// System.out.println(source.toString());
			BsonDocument sourceDocument = BsonDocument.parse(source.toString());
			// System.out.println(sourceDocument.toJson());
			String sourceDataName = sourceDocument.getString("name").getValue();
			// System.out.println(sourceDataName);
			sourceDataIds.add(sourceDataName);
			DataAccess DataAccess = new DataAccess(sourceDataName, frequency, memcachedClient);
			task.dataAccess.add(DataAccess);
			// System.out.println("next source");
		}

		BsonDocument ruleDocument = null;
		if (document.containsKey("rule")) {
			ruleDocument = document.getDocument("rule");
		}
		task.dataAssemble = new DataAssemble(resultDataName, type, frequency, memcachedClient, sourceDataIds,
				ruleDocument);

		// System.out.println("return");
		return task;
	}
}

class DataAccess {
	private String dataId;
	private int dataAccessIntervalInSeconds;
	private MemcachedClient memcachedClient;

	private static final String dataAccessURL = "http://127.0.0.1:8080/mavenWebTest/get/";

	public int getDataAccessIntervalInSeconds() {
		return dataAccessIntervalInSeconds;
	}

	public String getDataId() {
		return dataId;
	}

	public DataAccess(String dataId, int dataAccessIntervalInSeconds, MemcachedClient memcachedClient) {
		this.dataId = dataId;
		this.dataAccessIntervalInSeconds = dataAccessIntervalInSeconds;
		this.memcachedClient = memcachedClient;
	}

	public void run() {
		StringBuilder result = new StringBuilder();
		try {
			URL url = new URL(dataAccessURL + dataId);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String line;
			while ((line = reader.readLine()) != null) {
				result.append(line);
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		memcachedClient.set(dataId, 0, result.toString());
	}
}

class DataAssemble {
	private String resultDataId;
	private String type;
	private int dataAssembleIntervalInSeconds;
	private MemcachedClient memcachedClient;
	private List<String> sourceDataIds;
	private BsonDocument ruleDocument;

	public String getResultDataId() {
		return resultDataId;
	}

	public int getDataAssembleIntervalInSeconds() {
		return dataAssembleIntervalInSeconds;
	}

	public DataAssemble(String resultDataId, String type, int dataAssembleIntervalInSeconds,
			MemcachedClient memcachedClient, List<String> sourceDataIds, BsonDocument ruleDocument) {
		this.resultDataId = resultDataId;
		this.type = type;
		this.dataAssembleIntervalInSeconds = dataAssembleIntervalInSeconds;
		this.memcachedClient = memcachedClient;
		this.sourceDataIds = sourceDataIds;
		this.ruleDocument = ruleDocument;
	}

	public void run() {
		// if(ruleDocument == null){
		// System.out.println("no rule");
		// memcachedClient.set(resultDataId, 0, result.toString());
		// return;
		// }
		if (ruleDocument.isEmpty()) {
			StringBuilder result = new StringBuilder();
			for (String sourceDataId : sourceDataIds) {
				Object object = memcachedClient.get(sourceDataId);
				String data = (String) object;
				result.append(data);
			}
			// System.out.println(resultDataId + " is empty");
			memcachedClient.set(resultDataId, 0, result.toString());
			return;
		}
		System.out.println(resultDataId + ruleDocument.toJson());
		switch (ruleDocument.getString("ruleName").getValue()) {
		case "sort": {
			List<BsonDocument> sortSourceDataList = new ArrayList<>();
			for (String sourceDataId : sourceDataIds) {
				Object object = memcachedClient.get(sourceDataId);
				BsonDocument sourceDataObject = BsonDocument.parse((String) object);
				if (sourceDataObject.containsKey("content")) {
					BsonArray sourceDataArray = sourceDataObject.getArray("content");
					sortSourceDataList.add(BsonDocument.parse(sourceDataArray.get(0).toString()));
				}
			}
			//System.out.println(sortSourceDataList.toString());
			Collections.sort(sortSourceDataList,
					new Sort(ruleDocument.getString("key").getValue(), ruleDocument.getString("order").getValue()));

			BsonArray sourceDataArray = new BsonArray();
			sourceDataArray.addAll(sortSourceDataList);
			
			BsonDocument sortResultData = new BsonDocument();

			sortResultData.append("name", new BsonString(resultDataId));
			sortResultData.append("type", new BsonString(type));
			sortResultData.append("frequency", new BsonInt32(dataAssembleIntervalInSeconds));
			sortResultData.append("content", sourceDataArray);

			//System.out.println(resultDataId + " return " + sortResultData);
			memcachedClient.set(resultDataId, 0, sortResultData.toJson());
			return;
		}
		case "exp": {
			String expression = ruleDocument.getString("expression").getValue();
			String key = ruleDocument.getString("key").getValue();
			for (String sourceDataId : sourceDataIds) {
				Object object = memcachedClient.get(sourceDataId);
				
				String value = "";
				BsonDocument sourceDataObject = BsonDocument.parse((String) object);
				if (sourceDataObject.containsKey("content")) {
					BsonArray sourceDataArray = sourceDataObject.getArray("content");
					BsonDocument sourceDataContent = BsonDocument.parse(sourceDataArray.get(0).toString());
					if (sourceDataContent.get(key).isInt32()) {
						value = String.valueOf(sourceDataContent.getInt32(key).getValue());
					} else if (sourceDataContent.get(key).isDouble()) {
						value = String.valueOf(sourceDataContent.getInt32(key).getValue());
					}
				}
				expression = expression.replaceAll(sourceDataId, value);
				System.out.println(expression);
			}
			BsonDocument expResultData = new BsonDocument();
			Calculator calculator = new Calculator();
			try {
				System.out.println(expression);
				double result = calculator.calculate(expression);
				System.out.println(result);
				expResultData.append("name", new BsonString(resultDataId));
				expResultData.append("type", new BsonString(type));
				expResultData.append("frequency", new BsonInt32(dataAssembleIntervalInSeconds));
				expResultData.append("content", new BsonDouble(result));
				System.out.println(resultDataId + " " + expResultData);
				memcachedClient.set(resultDataId, 0, expResultData.toJson());
				return;
			} catch (Exception e) {
				// TODO: handle exception
				if (e instanceof java.lang.ArithmeticException) {
					System.out.println("/ by zero");

					expResultData.append("name", new BsonString(resultDataId));
					expResultData.append("type", new BsonString(type));
					expResultData.append("frequency", new BsonInt32(dataAssembleIntervalInSeconds));
					expResultData.append("content", new BsonString("/ by zero"));

					memcachedClient.set(resultDataId, 0, expResultData.toJson());
					return;
				} else if (e instanceof java.util.EmptyStackException) {
					System.out.println("表达式不完整");

					expResultData.append("name", new BsonString(resultDataId));
					expResultData.append("type", new BsonString(type));
					expResultData.append("frequency", new BsonInt32(dataAssembleIntervalInSeconds));
					expResultData.append("content", new BsonString("表达式不完整"));

					memcachedClient.set(resultDataId, 0, expResultData.toJson());
					return;
				} else if (e instanceof java.lang.NumberFormatException) {
					System.out.println("表达式中包含非数字部分");

					expResultData.append("name", new BsonString(resultDataId));
					expResultData.append("type", new BsonString(type));
					expResultData.append("frequency", new BsonInt32(dataAssembleIntervalInSeconds));
					expResultData.append("content", new BsonString("表达式中包含非数字部分"));

					memcachedClient.set(resultDataId, 0, expResultData.toJson());
					return;
				}
			}
		}
		}

		// System.out.println(result.toString());
		// memcachedClient.set(resultDataId, 0);
	}
}

class Sort implements Comparator<BsonDocument> {
	private String key;
	private String order;

	public Sort(String key, String order) {
		this.key = key;
		this.order = order;
	}

	@Override
	public int compare(BsonDocument arg0, BsonDocument arg1) {
		double first = 0;
		double second = 0;
		if (arg0.get(key).isInt32()) {
			first = arg0.getInt32(key).getValue();
		} else if (arg0.get(key).isDouble()) {
			first = arg0.getDouble(key).getValue();
		}
		if (arg1.get(key).isInt32()) {
			second = arg1.getInt32(key).getValue();
		} else if (arg1.get(key).isDouble()) {
			second = arg1.getDouble(key).getValue();
		}
		switch (order) {
		case "asc":
			return (int) (first - second);
		case "desc":
			return (int) (second - first);
		case "unorder":
			return 0;
		default:
			return 0;
		}
	}
}
