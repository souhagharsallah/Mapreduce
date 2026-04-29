package common;
import java.io.Serializable;
import java.util.Map;
public class FinalResult  implements Serializable {
    private int reducerId;
    private Map<String, Integer> finalCounts;

    public FinalResult(int reducerId, Map<String, Integer> finalCounts) {
        this.reducerId = reducerId;
        this.finalCounts = finalCounts;
    }

    public int getReducerId() {
        return reducerId;
    }

    public Map<String, Integer> getFinalCounts() {
        return finalCounts;
    }
}
