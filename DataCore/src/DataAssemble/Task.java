package DataAssemble;

import net.spy.memcached.MemcachedClient;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Quanyang Liu on 10/18/16.
 */
public class Task {
    public List<DataAccessTask> dataAccessTask;
    public DataAssembleTask dataAssembleTask;

    public static Task FromDocument(
    		BsonDocument document, 
    		MemcachedClient memcachedClient
    		) {
    	Task task = new Task();
    	System.out.println(document.toJson());
    	int frequency = document.getInt32("frequency").getValue();
    	String resultDataName = document.getString("name").getValue();
    	
    	List<String> sourceDataIds = new ArrayList<>();
    	
    	
    	for (Object source : document.getArray("sourceDataList")) {
    		BsonDocument sourceDocument = (BsonDocument) source;
    		String sourceDataName = sourceDocument.getString("name").getValue();
    		sourceDataIds.add(sourceDataName);
    		DataAccessTask dataAccessTask = new DataAccessTask(
    				sourceDataName, 
    				frequency, 
    				memcachedClient
    				);
    		task.dataAccessTask.add(dataAccessTask);
    	}
    	
    	task.dataAssembleTask = new DataAssembleTask(
    			resultDataName,
    			frequency,
    			memcachedClient,
    			sourceDataIds
    			);
    	
        return task;
    }
}

class DataAccessTask {
    private String dataId;
    private int dataAccessIntervalInSeconds;
    private MemcachedClient memcachedClient;
    
    private static final String dataAccessURL =
    		"http:127.0.0.1:8080/mavenWebTest/get/";

    public int getDataAccessIntervalInSeconds() {
        return dataAccessIntervalInSeconds;
    }

    public String getDataId() {
        return dataId;
    }

    public DataAccessTask(
            String dataId,
            int dataAccessIntervalInSeconds,
            MemcachedClient memcachedClient
    ) {
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
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(conn.getInputStream())
							);
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

class DataAssembleTask {
    private String resultDataId;
    private int dataAssembleIntervalInSeconds;
    private MemcachedClient memcachedClient;
    private List<String> sourceDataIds;

    public String getResultDataId() {
        return resultDataId;
    }

    public int getDataAssembleIntervalInSeconds() {
        return dataAssembleIntervalInSeconds;
    }

    public DataAssembleTask(
            String resultDataId,
            int dataAssembleIntervalInSeconds,
            MemcachedClient memcachedClient,
            List<String> sourceDataIds) {
        this.resultDataId = resultDataId;
        this.dataAssembleIntervalInSeconds = dataAssembleIntervalInSeconds;
        this.memcachedClient = memcachedClient;
        this.sourceDataIds = sourceDataIds;
    }

    public void run() {
    	StringBuilder result = new StringBuilder();
        for (String sourceDataId : sourceDataIds) {
            String data = (String) memcachedClient.get(sourceDataId);
            result.append(data);
        }
        memcachedClient.add(resultDataId, 0, result.toString());
    }
}

