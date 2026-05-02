package common;

import java.io.Serializable;

public class ChunkInfo implements Serializable {
    private int chunkId;
    private String content;
    private int numReducers;
    private java.util.List<String> reducerHosts;
    private java.util.List<Integer> reducerPorts;

    public ChunkInfo(int chunkId, String content, int numReducers,
                     java.util.List<String> reducerHosts,
                     java.util.List<Integer> reducerPorts) {
        this.chunkId = chunkId;
        this.content = content;
        this.numReducers = numReducers;
        this.reducerHosts = reducerHosts;
        this.reducerPorts = reducerPorts;
    }

    public int getChunkId() {
        return chunkId;
    }

    public String getContent() {
        return content;
    }

    public int getNumReducers() {
        return numReducers;
    }

    public java.util.List<String> getReducerHosts() {
        return reducerHosts;
    }

    public java.util.List<Integer> getReducerPorts() {
        return reducerPorts;
    }
}