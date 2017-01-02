package DataAssemble;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created by Quanyang Liu on 10/18/16.
 */
public class DataAssembleService {
    private ScheduledExecutorService scheduledExecutorService;
    private Map<String, ScheduledFuture> DataAssembles;

    public DataAssembleService(int threadNum) {
        scheduledExecutorService = Executors.newScheduledThreadPool(threadNum);
        DataAssembles = new HashMap<>();
    }

    public void AppendTask(DataAssemble task) {
        if (DataAssembles.containsKey(task.getResultDataId())) {
            DataAssembles.get(task.getResultDataId()).cancel(false);
        }

        ScheduledFuture scheduledFuture =
                scheduledExecutorService.scheduleAtFixedRate(task::run, 0,
                        task.getDataAssembleIntervalInSeconds(), TimeUnit.SECONDS);
        DataAssembles.put(task.getResultDataId(), scheduledFuture);
    }
}
