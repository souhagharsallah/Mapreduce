package coordinator;
import common.Message;
import common.MessageType;
import common.TaskInfo;

import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import common.FinalResult;

import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

public class Coordinator {
    private final List<String> mapHosts = new ArrayList<>();
    private final List<Integer> mapPorts = new ArrayList<>();

    private final List<String> reducerHosts = new ArrayList<>();
    private final List<Integer> reducerPorts = new ArrayList<>();

    public void addMapWorker(String host, int port) {
        mapHosts.add(host);
        mapPorts.add(port);
    }

    public void addReducerWorker(String host, int port) {
        reducerHosts.add(host);
        reducerPorts.add(port);
    }

    public void dispatchMapTasks(List<String> files) {
        int numMaps = files.size();

        for (int i = 0; i < numMaps; i++) {
            String file = files.get(i);
            String mapHost = mapHosts.get(i);
            int mapPort = mapPorts.get(i);

            TaskInfo task = new TaskInfo(file, reducerPorts.size(), reducerHosts, reducerPorts);
            sendMapTask(mapHost, mapPort, task);

            System.out.println("Coordinator sent file " + file + " to MapWorker " + i);
        }
    }

    private void sendMapTask(String host, int port, TaskInfo task) {
        try (
                Socket socket = new Socket(host, port);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())
        ) {
            out.writeObject(new Message(MessageType.MAP_TASK, task));
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Coordinator coordinator = new Coordinator();

        coordinator.addMapWorker("localhost", 5001);
        coordinator.addMapWorker("localhost", 5002);

        coordinator.addReducerWorker("localhost", 6001);
        coordinator.addReducerWorker("localhost", 6002);

        List<String> files = List.of(
                "data/file1.txt",
                "data/file2.txt"
        );

        coordinator.dispatchMapTasks(files);
        coordinator.waitForFinalResults(2);
    }
    public void waitForFinalResults(int numberOfReducers) {
        Map<String, Integer> globalCounts = new HashMap<>();
        int receivedReducers = 0;

        try (ServerSocket serverSocket = new ServerSocket(7000)) {
            System.out.println("Coordinator waiting for final results on port 7000...");

            while (receivedReducers < numberOfReducers) {
                Socket socket = serverSocket.accept();

                try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
                    Message msg = (Message) in.readObject();

                    if (msg.getType() == MessageType.FINAL_RESULT) {
                        FinalResult result = (FinalResult) msg.getPayload();

                        for (Map.Entry<String, Integer> entry : result.getFinalCounts().entrySet()) {
                            globalCounts.put(
                                    entry.getKey(),
                                    globalCounts.getOrDefault(entry.getKey(), 0) + entry.getValue()
                            );
                        }

                        receivedReducers++;

                        System.out.println("Coordinator received result from Reducer "
                                + result.getReducerId()
                                + " (" + receivedReducers + "/" + numberOfReducers + ")");
                    }
                }
            }

            System.out.println("===== GLOBAL FINAL RESULT =====");
            for (Map.Entry<String, Integer> entry : globalCounts.entrySet()) {
                System.out.println(entry.getKey() + " -> " + entry.getValue());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
