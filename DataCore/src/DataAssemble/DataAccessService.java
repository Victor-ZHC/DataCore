package DataAssemble;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created by Quanyang Liu on 10/18/16.
 */
public class DataAccessService {
    class ScheduledTask {
        public ScheduledFuture scheduledFuture;
        public int periodInMinutes;
    }
    private Map<String, ScheduledTask> DataAccesss;
    private ScheduledExecutorService scheduledExecutorService;

    public DataAccessService(int threadNum) {
        DataAccesss = new HashMap<>();
        scheduledExecutorService = Executors.newScheduledThreadPool(threadNum);
    }

    public void AppendTask(List<DataAccess> tasks) {
        for (DataAccess task : tasks) {
            if (DataAccesss.containsKey(task.getDataId())) {
                ScheduledTask scheduledTask =
                        DataAccesss.get(task.getDataId());

                if (scheduledTask.periodInMinutes >
                        task.getDataAccessIntervalInSeconds()) {
                    scheduledTask.scheduledFuture.cancel(false);
                }
            }

            ScheduledTask scheduledTask = new ScheduledTask();
            scheduledTask.scheduledFuture =
                    scheduledExecutorService.scheduleAtFixedRate(task::run, 0,
                            task.getDataAccessIntervalInSeconds(), TimeUnit.SECONDS);
            scheduledTask.periodInMinutes = task.getDataAccessIntervalInSeconds();

            DataAccesss.put(task.getDataId(), scheduledTask);
        }
    }
}