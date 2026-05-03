package common;
import java.io.Serializable;
import java.util.List;
public class TaskInfo implements Serializable {
    private int taskId;
    private String filePath;
    private int numReducers;
    private List<String> reducerHosts;
    private List<Integer> reducerPorts;

    public TaskInfo(int taskId, String filePath, int numReducers, List<String> reducerHosts, List<Integer> reducerPorts) {
        this.taskId = taskId;
        this.filePath = filePath;
        this.numReducers = numReducers;
        this.reducerHosts = reducerHosts;
        this.reducerPorts = reducerPorts;
    }

    public int getTaskId() {
        return taskId;
    }

    public String getFilePath() {
        return filePath;
    }

    public int getNumReducers() {
        return numReducers;
    }

    public List<String> getReducerHosts() {
        return reducerHosts;
    }

    public List<Integer> getReducerPorts() {
        return reducerPorts;
    }
}
