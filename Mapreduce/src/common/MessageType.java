package common;

import java.io.Serializable;

public enum MessageType implements Serializable {
    MAP_TASK,
    MAP_DONE,
    INTERMEDIATE_DATA,
    RESULT,
    FINAL_RESULT
}