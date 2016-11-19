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
    private Map<String, ScheduledFuture> dataAssembleTasks;

    public DataAssembleService(int threadNum) {
        scheduledExecutorService = Executors.newScheduledThreadPool(threadNum);
        dataAssembleTasks = new HashMap<>();
    }

    public void AppendTask(DataAssembleTask task) {
        if (dataAssembleTasks.containsKey(task.getResultDataId())) {
            dataAssembleTasks.get(task.getResultDataId()).cancel(false);
        }

        ScheduledFuture scheduledFuture =
                scheduledExecutorService.scheduleAtFixedRate(task::run, 0,
                        task.getDataAssembleIntervalInSeconds(), TimeUnit.SECONDS);
        dataAssembleTasks.put(task.getResultDataId(), scheduledFuture);
    }
}
