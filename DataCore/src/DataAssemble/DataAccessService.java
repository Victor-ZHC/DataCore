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
    private Map<String, ScheduledTask> dataAccessTasks;
    private ScheduledExecutorService scheduledExecutorService;

    public DataAccessService(int threadNum) {
        dataAccessTasks = new HashMap<>();
        scheduledExecutorService = Executors.newScheduledThreadPool(threadNum);
    }

    public void AppendTask(List<DataAccessTask> tasks) {
        for (DataAccessTask task : tasks) {
            if (dataAccessTasks.containsKey(task.getDataId())) {
                ScheduledTask scheduledTask =
                        dataAccessTasks.get(task.getDataId());

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

            dataAccessTasks.put(task.getDataId(), scheduledTask);
        }
    }
}