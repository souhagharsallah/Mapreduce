package common;
import java.io.Serializable;
import java.util.Map;
public class IntermediateData implements Serializable {
    private int mapId;
    private int reducerId;
    private Map<String, Integer> wordCounts;

    public IntermediateData(int mapId, int reducerId, Map<String, Integer> wordCounts) {
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


}
