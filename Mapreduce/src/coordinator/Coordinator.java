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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.FileWriter;

public class Coordinator {
    private final List<String> mapHosts = new ArrayList<>();
    private final List<Integer> mapPorts = new ArrayList<>();

    private final List<String> reducerHosts = new ArrayList<>();
    private final List<Integer> reducerPorts = new ArrayList<>();
    private final List<Process> workerProcesses = new ArrayList<>();

    private void startMapWorkers(int numberOfMappers) {
        for (int i = 0; i < numberOfMappers; i++) {
            int port = 5001 + i;

            addMapWorker("localhost", port);

            try {
                Process process = new ProcessBuilder(
                        "java", "-cp", "out", "Map.MapWorker",
                        String.valueOf(i),
                        String.valueOf(port)
                )
                        .inheritIO()
                        .start();

                workerProcesses.add(process);

                System.out.println("Coordinator started MapWorker " + i + " on port " + port);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    private void startReduceWorkers(int numberOfReducers, int expectedMaps) {
        for (int i = 0; i < numberOfReducers; i++) {
            int port = 6001 + i;

            addReducerWorker("localhost", port);

            try {
                Process process = new ProcessBuilder(
                        "java", "-cp", "out", "Reduce.ReduceWorker",
                        String.valueOf(i),
                        String.valueOf(port),
                        String.valueOf(expectedMaps)
                )
                        .inheritIO()
                        .start();

                workerProcesses.add(process);

                System.out.println("Coordinator started ReduceWorker " + i + " on port " + port);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
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
            int mapperIndex = i % mapHosts.size();
            String mapHost = mapHosts.get(mapperIndex);
            int mapPort = mapPorts.get(mapperIndex);

            TaskInfo task = new TaskInfo(file, reducerPorts.size(), reducerHosts, reducerPorts);
            sendMapTask(mapHost, mapPort, task);

            System.out.println("Coordinator sent file " + file + " to MapWorker " + mapperIndex);
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

    public static void main(String[] args) throws IOException {
        Coordinator coordinator = new Coordinator();

        String content = Files.readString(Paths.get("data/bigfile.txt"));

        int CHUNK_SIZE = 1024 * 1024;
        List<String> chunks = splitBySize(content, CHUNK_SIZE);

        Files.createDirectories(Paths.get("data/chunks"));

        List<String> files = new ArrayList<>();

        for (int i = 0; i < chunks.size(); i++) {
            Path chunkPath = Paths.get("data/chunks/chunk_" + i + ".txt");
            Files.writeString(chunkPath, chunks.get(i));
            files.add(chunkPath.toString());
        }

        int MAX_MAPPERS = 8;
        int MAX_REDUCERS = 4;

        int numberOfMappers = Math.min(chunks.size(), MAX_MAPPERS);
        int numberOfReducers = Math.min(numberOfMappers, MAX_REDUCERS);

        Thread resultThread = new Thread(() -> coordinator.waitForFinalResults(numberOfReducers));
        resultThread.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        coordinator.startReduceWorkers(numberOfReducers, chunks.size());
        coordinator.startMapWorkers(numberOfMappers);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //temps de execution
        long startTime = System.currentTimeMillis();

        coordinator.dispatchMapTasks(files);

        try {
            resultThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //fin temps de execution
        long endTime = System.currentTimeMillis();

        System.out.println("Temps d'exécution total : " + (endTime - startTime) + " ms");
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

            Files.createDirectories(Paths.get("output"));

            try (BufferedWriter writer = new BufferedWriter(new FileWriter("output/result.txt"))) {
                int count = 0;

                for (Map.Entry<String, Integer> entry : globalCounts.entrySet()) {
                    String line = entry.getKey() + " -> " + entry.getValue();

                    writer.write(line);
                    writer.newLine();

                    if (count < 10) {
                        System.out.println(line);
                    }

                    count++;
                }

                System.out.println("...");
                System.out.println("Résultat complet sauvegardé dans : output/result.txt");
                System.out.println("Nombre total de mots différents : " + globalCounts.size());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static List<String> splitBySize(String content, int chunkSize) {
        List<String> chunks = new ArrayList<>();

        int start = 0;

        while (start < content.length()) {
            int end = Math.min(start + chunkSize, content.length());

            if (end < content.length()) {
                while (end < content.length() && !Character.isWhitespace(content.charAt(end))) {
                    end++;
                }
            }

            String chunk = content.substring(start, end).trim();

            if (!chunk.isEmpty()) {
                chunks.add(chunk);
            }

            start = end;
        }

        return chunks;
    }

}
