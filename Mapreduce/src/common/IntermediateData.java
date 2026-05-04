package common;
import java.io.Serializable;
import java.util.Map;
public class IntermediateData implements Serializable {
    private int mapId;
    private int reducerId;
    private int taskId;
    private Map<String, Integer> wordCounts;

    public IntermediateData(int taskId, int mapId, int reducerId, Map<String, Integer> wordCounts) {
        this.taskId = taskId;
        this.mapId = mapId;
        this.reducerId = reducerId;
        this.wordCounts = wordCounts;
    }

    public int getMapId() {
        return mapId;
    }

    public int getReducerId() {
        return reducerId;
    }

    public Map<String, Integer> getWordCounts() {
        return wordCounts;
    }

    public int getTaskId() {
        return taskId;
    }

}
