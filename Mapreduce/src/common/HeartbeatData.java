package common;
import java.io.Serializable;

public class HeartbeatData implements Serializable {
    private String workerType; // Sera "MAP" ou "REDUCE"
    private int workerId;

    public HeartbeatData(String workerType, int workerId) {
        this.workerType = workerType;
        this.workerId = workerId;
    }

    public String getWorkerType() { return workerType; }
    public int getWorkerId() { return workerId; }
}

