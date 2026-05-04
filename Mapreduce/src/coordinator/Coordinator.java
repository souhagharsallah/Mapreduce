package coordinator;

import Map.MapWorker;
import Reduce.ReduceWorker;
import common.*;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Coordinator {

    // ---------------- WORKERS ----------------

    static class WorkerStatus {
        long lastHeartbeat;
        String state; // ACTIVE, SUSPECTED, DEAD

        public WorkerStatus(long lastHeartbeat, String state) {
            this.lastHeartbeat = lastHeartbeat;
            this.state = state;
        }
    }

    private final Map<String, WorkerStatus> workers = new ConcurrentHashMap<>();

    // ---------------- REGISTRY ----------------

    private final List<String> mapHosts = new ArrayList<>();
    private final List<Integer> mapPorts = new ArrayList<>();

    private final List<String> reducerHosts = new ArrayList<>();
    private final List<Integer> reducerPorts = new ArrayList<>();

    // ---------------- REGISTER WORKERS ----------------

    public void addMapWorker(String host, int port) {
        mapHosts.add(host);
        mapPorts.add(port);
    }

    public void addReducerWorker(String host, int port) {
        reducerHosts.add(host);
        reducerPorts.add(port);
    }

    // ---------------- MAP TASK ----------------

    public void dispatchMapTasks(List<String> files) {

        for (int i = 0; i < files.size(); i++) {

            String file = files.get(i);
            int mapperIndex = i % mapHosts.size();

            String host = mapHosts.get(mapperIndex);
            int port = mapPorts.get(mapperIndex);

            TaskInfo task = new TaskInfo(
                    file,
                    reducerPorts.size(),
                    reducerHosts,
                    reducerPorts
            );

            sendMapTask(host, port, task);

            System.out.println("Sent file " + file + " to MapWorker " + i);
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

    // ---------------- HEARTBEAT LISTENER ----------------

    public void startHeartbeatListener() {

        Thread t = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(7001)) {

                System.out.println("Coordinator listening HEARTBEATS on 7001...");

                while (true) {

                    Socket socket = serverSocket.accept();
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                    Message msg = (Message) in.readObject();

                    if (msg.getType() == MessageType.HEARTBEAT) {

                        HeartbeatData data = (HeartbeatData) msg.getPayload();
                        String workerName = data.getWorkerType() + "_" + data.getWorkerId();

                        workers.put(workerName,
                                new WorkerStatus(System.currentTimeMillis(), "ACTIVE"));
                    }

                    socket.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        t.setDaemon(true);
        t.start();
    }

    // ---------------- FAILURE DETECTOR (ROBUSTE) ----------------

    public void startFailureDetector() {

        Thread t = new Thread(() -> {

            while (true) {
                try {

                    Thread.sleep(5000);
                    long now = System.currentTimeMillis();

                    for (Map.Entry<String, WorkerStatus> entry : workers.entrySet()) {

                        String name = entry.getKey();
                        WorkerStatus ws = entry.getValue();

                        long delay = now - ws.lastHeartbeat;

                        if (delay > 10000 && delay < 20000) {
                            ws.state = "SUSPECTED";
                            System.out.println("SUSPECTED: " + name);
                        }

                        if (delay > 20000) {
                            ws.state = "DEAD";
                            System.err.println(" DEAD: " + name);
                            workers.remove(name);
                        }
                    }

                } catch (Exception ignored) {}
            }

        });

        t.setDaemon(true);
        t.start();
    }

    // ---------------- MAIN ----------------

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        Coordinator coordinator = new Coordinator();

        coordinator.startHeartbeatListener();
        coordinator.startFailureDetector();

        String file = "src/data/bigfile.txt";

        List<String> chunks = coordinator.splitFile(file, 50);

        int numMaps = chunks.size();
        int numReducers = Math.max(2, Runtime.getRuntime().availableProcessors());

        // start reducers
        for (int i = 0; i < numReducers; i++) {
            coordinator.addReducerWorker("localhost", 6000 + i);
            coordinator.startWorkerProcess(
                    ReduceWorker.class,
                    String.valueOf(i),
                    String.valueOf(6000 + i),
                    String.valueOf(numMaps)
            );
        }

        // start mappers
        for (int i = 0; i < numMaps; i++) {
            coordinator.addMapWorker("localhost", 5000 + i);

            coordinator.startWorkerProcess(
                    MapWorker.class,
                    String.valueOf(i),
                    String.valueOf(5000 + i)
            );
        }

        try { Thread.sleep(2000); } catch (Exception ignored) {}

        long mapStart = System.currentTimeMillis();
        coordinator.dispatchMapTasks(chunks);

        long reduceStart = System.currentTimeMillis();
        coordinator.waitForFinalResults(numReducers);

        long endTime = System.currentTimeMillis(); // FIN
        long duration = endTime - startTime;
        System.out.println("⏱ Phase Map    : " + (reduceStart - mapStart) + " ms");
        System.out.println("⏱ Phase Reduce : " + (endTime - reduceStart) + " ms");
        System.out.println(" Temps total : " + duration + " ms (" + (duration / 1000.0) + " s)");
    }

    // ---------------- FILE SPLIT ----------------

    public List<String> splitFile(String path, int sizeMB) {

        List<String> files = new ArrayList<>();

        try {

            File f = new File(path);
            BufferedReader br = new BufferedReader(new FileReader(f));

            String line;
            int index = 1;
            long max = sizeMB * 1024 * 1024;
            long current = 0;

            String out = path + "_part" + index + ".txt";
            BufferedWriter bw = new BufferedWriter(new FileWriter(out));
            files.add(out);

            while ((line = br.readLine()) != null) {

                bw.write(line);
                bw.newLine();

                current += line.getBytes().length;

                if (current > max) {
                    bw.close();
                    index++;
                    out = path + "_part" + index + ".txt";
                    bw = new BufferedWriter(new FileWriter(out));
                    files.add(out);
                    current = 0;
                }
            }

            bw.close();
            br.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return files;
    }

    // ---------------- PROCESS LAUNCH ----------------

    private void startWorkerProcess(Class<?> clazz, String... args) {

        try {
            String java = System.getProperty("java.home") + "/bin/java";
            String cp = System.getProperty("java.class.path");

            List<String> cmd = new ArrayList<>();
            cmd.add(java);
            cmd.add("-cp");
            cmd.add(cp);
            cmd.add(clazz.getName());
            cmd.addAll(Arrays.asList(args));

            new ProcessBuilder(cmd).inheritIO().start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ---------------- RESULTS ----------------

    public void waitForFinalResults(int reducers) {

        Map<String, Integer> global = new TreeMap<>();
        int received = 0;

        try (ServerSocket server = new ServerSocket(7000)) {

            System.out.println("Waiting FINAL RESULTS...");

            while (received < reducers) {

                Socket s = server.accept();
                ObjectInputStream in = new ObjectInputStream(s.getInputStream());

                Message msg = (Message) in.readObject();

                if (msg.getType() == MessageType.FINAL_RESULT) {

                    FinalResult r = (FinalResult) msg.getPayload();

                    for (Map.Entry<String, Integer> e : r.getFinalCounts().entrySet()) {
                        global.put(e.getKey(),
                                global.getOrDefault(e.getKey(), 0) + e.getValue());
                    }

                    received++;
                    // Le Reducer a envoyé son résultat, il a fini.
                    // On prévient le FailureDetector d'arrêter de le surveiller !
                    String workerName = "REDUCER_" + r.getReducerId();
                    workers.remove(workerName);
                    System.out.println("Coordinateur : Le Reducer " + r.getReducerId() + " a terminé avec succès. Arrêt de la surveillance.");
                    // -----------------------------

                }

                s.close();
            }

            try (BufferedWriter w = new BufferedWriter(new FileWriter("final.txt"))) {

                for (Map.Entry<String, Integer> e : global.entrySet()) {
                    w.write(e.getKey() + " -> " + e.getValue());
                    w.newLine();
                }
            }

            System.out.println("DONE ");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}