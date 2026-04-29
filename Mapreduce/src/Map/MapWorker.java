package Map;
import common.IntermediateData;
import common.Message;
import common.MessageType;
import common.TaskInfo;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class MapWorker {
    private int mapId;
    private int listenPort;
    public MapWorker(int mapId, int listenPort) {
        this.mapId = mapId;
        this.listenPort = listenPort;
    }
    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(listenPort);
        System.out.println("MapWorker " + mapId + " listening on port " + listenPort);

        while (true) {
            Socket socket = serverSocket.accept();
            new Thread(() -> handleConnection(socket)).start();
        }
    }
    private void handleConnection(Socket socket) {
        try (
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())
        ) {
            Message msg = (Message) in.readObject();

            if (msg.getType() == MessageType.MAP_TASK) {
                TaskInfo task = (TaskInfo) msg.getPayload();
                processTask(task);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void processTask(TaskInfo task) {
        try {
            String content = Files.readString(Paths.get(task.getFilePath()));

            content = content.toLowerCase();
            content = content.replaceAll("[^a-zàâçéèêëîïôûùüÿñæœ0-9 ]", " ");

            String[] words = content.split("\\s+");

            Map<String, Integer> localCounts = new HashMap<>();

            for (String word : words) {
                if (word == null || word.isBlank()) continue;
                localCounts.put(word, localCounts.getOrDefault(word, 0) + 1);
            }

            Map<Integer, Map<String, Integer>> partitions = new HashMap<>();

            for (int i = 0; i < task.getNumReducers(); i++) {
                partitions.put(i, new HashMap<>());
            }

            for (Map.Entry<String, Integer> entry : localCounts.entrySet()) {
                String word = entry.getKey();
                int count = entry.getValue();

                int reducerId = Math.abs(word.hashCode()) % task.getNumReducers();
                partitions.get(reducerId).put(word, count);
            }
            for (int reducerId = 0; reducerId < task.getNumReducers(); reducerId++) {
                sendToReducer(
                        task.getReducerHosts().get(reducerId),
                        task.getReducerPorts().get(reducerId),
                        new IntermediateData(mapId, reducerId, partitions.get(reducerId))
                );
            }

            System.out.println("MapWorker " + mapId + " finished file: " + task.getFilePath());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void sendToReducer(String host, int port, IntermediateData data) {
        try (
                Socket socket = new Socket(host, port);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())
        ) {
            out.writeObject(new Message(MessageType.INTERMEDIATE_DATA, data));
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        int mapId = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);

        MapWorker worker = new MapWorker(mapId, port);
        worker.start();
    }

        }


