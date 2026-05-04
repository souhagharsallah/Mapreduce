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

    static class WorkerStatus {
        long lastHeartbeat;
        String state;

        public WorkerStatus(long lastHeartbeat, String state) {
            this.lastHeartbeat = lastHeartbeat;
            this.state = state;
        }
    }

    private final Map<String, WorkerStatus> workers = new ConcurrentHashMap<>();

    static class TaskExecutionInfo {
        TaskInfo task;
        long startTime;
        boolean completed;

        public TaskExecutionInfo(TaskInfo task) {
            this.task = task;
            this.startTime = System.currentTimeMillis();
            this.completed = false;
        }
    }

    private final Map<Integer, TaskExecutionInfo> tasks = new ConcurrentHashMap<>();
    private int nextWorkerId = 1000;
    private int nextSpeculativePort = 8000;
    private final List<String> mapHosts = new ArrayList<>();
    private final List<Integer> mapPorts = new ArrayList<>();

    private final List<String> reducerHosts = new ArrayList<>();
    private final List<Integer> reducerPorts = new ArrayList<>();

    private final List<Process> workerProcesses = new ArrayList<>();

    public void addMapWorker(String host, int port) {
        mapHosts.add(host);
        mapPorts.add(port);
    }

    public void addReducerWorker(String host, int port) {
        reducerHosts.add(host);
        reducerPorts.add(port);
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

    public void dispatchMapTasks(List<String> files) {
        Queue<String> taskQueue = new LinkedList<>(files);

        int workerCount = mapHosts.size();
        int taskId = 0;

        while (!taskQueue.isEmpty()) {
            for (int i = 0; i < workerCount; i++) {

                if (taskQueue.isEmpty()) {
                    break;
                }

                String file = taskQueue.poll();

                String host = mapHosts.get(i);
                int port = mapPorts.get(i);

                TaskInfo task = new TaskInfo(
                        taskId,
                        file,
                        reducerPorts.size(),
                        reducerHosts,
                        reducerPorts
                );

                tasks.put(taskId, new TaskExecutionInfo(task));

                sendMapTask(host, port, task);

                System.out.println("Sent file " + file + " as Task " + taskId + " to MapWorker " + i);

                taskId++;
            }

            try {
                Thread.sleep(100);
            } catch (Exception ignored) {}
        }
    }
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

                        workers.put(
                                workerName,
                                new WorkerStatus(System.currentTimeMillis(), "ACTIVE")
                        );
                    }
                    else if (msg.getType() == MessageType.MAP_DONE) {
                        int taskId = (Integer) msg.getPayload();
                        TaskExecutionInfo tInfo = tasks.get(taskId);
                        if (tInfo != null && !tInfo.completed) {
                            tInfo.completed = true;
                            System.out.println("Coordinator: Map Task " + taskId + " marked as COMPLETED.");
                        }
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
    public void startSpeculativeExecutionTracker() {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(5000);
                    long now = System.currentTimeMillis();

                    for (Map.Entry<Integer, TaskExecutionInfo> entry : tasks.entrySet()) {
                        TaskExecutionInfo tInfo = entry.getValue();

                        // If task is not completed and has been running for > 15 seconds
                        if (!tInfo.completed && (now - tInfo.startTime) > 30000) {
                            System.out.println("⚠️ SPECULATIVE EXECUTION: Task " + tInfo.task.getTaskId() + " is taking too long! Launching clone...");

                            // Reset start time so we don't keep launching clones every 5s
                            tInfo.startTime = now;

                            // Launch a new worker
                            int newWorkerId = nextWorkerId++;
                            int newPort = nextSpeculativePort++;

                            startWorkerProcess(
                                    MapWorker.class,
                                    String.valueOf(newWorkerId),
                                    String.valueOf(newPort)
                            );

                            // Wait a bit for the new worker to start
                            Thread.sleep(1000);

                            // Send the task to the new worker
                            sendMapTask("localhost", newPort, tInfo.task);
                        }
                    }
                } catch (Exception ignored) {}
            }
        });
        t.setDaemon(true);
        t.start();
    }

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
                            System.err.println("DEAD: " + name);
                            workers.remove(name);
                        }
                    }

                } catch (Exception ignored) {}
            }
        });

        t.setDaemon(true);
        t.start();
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        Coordinator coordinator = new Coordinator();

        coordinator.startHeartbeatListener();
        coordinator.startFailureDetector();
        coordinator.startSpeculativeExecutionTracker();

        String file = "src/data/livre.txt";

        List<String> chunks = coordinator.splitFile(file, 50);

        int numChunks = chunks.size();

        int cpuCores = Runtime.getRuntime().availableProcessors();
        long maxMemoryMB = Runtime.getRuntime().maxMemory() / (1024 * 1024);

        int maxMappersByCpu = cpuCores;
        int maxMappersByMemory = (int) Math.max(1, maxMemoryMB / 256);

        int maxMappersMachine = Math.min(maxMappersByCpu, maxMappersByMemory);

        int numMaps = Math.min(numChunks, maxMappersMachine);

        long fileSizeBytes = new File(file).length();
        double fileSizeMB = fileSizeBytes / (1024.0 * 1024.0);

        int maxReducersByCpu = cpuCores;
        int maxReducersByMemory = (int) Math.max(1, maxMemoryMB / 256);

        int maxReducersMachine = Math.min(maxReducersByCpu, maxReducersByMemory);

        int reducersByData = (int) Math.ceil(fileSizeMB / 256.0);

        int numReducers = Math.max(1, Math.min(maxReducersMachine, reducersByData));

        System.out.println("File size MB         : " + fileSizeMB);
        System.out.println("CPU cores            : " + cpuCores);
        System.out.println("Max memory MB        : " + maxMemoryMB);
        System.out.println("Number of chunks     : " + numChunks);
        System.out.println("Max mappers machine  : " + maxMappersMachine);
        System.out.println("Number of MapWorkers : " + numMaps);
        System.out.println("Max reducers machine : " + maxReducersMachine);
        System.out.println("Number of Reducers   : " + numReducers);

        for (int i = 0; i < numReducers; i++) {
            coordinator.addReducerWorker("localhost", 6000 + i);

            coordinator.startWorkerProcess(
                    ReduceWorker.class,
                    String.valueOf(i),
                    String.valueOf(6000 + i),
                    String.valueOf(numChunks)
            );
        }

        for (int i = 0; i < numMaps; i++) {
            coordinator.addMapWorker("localhost", 5000 + i);

            coordinator.startWorkerProcess(
                    MapWorker.class,
                    String.valueOf(i),
                    String.valueOf(5000 + i)
            );
        }

        try {
            Thread.sleep(2000);
        } catch (Exception ignored) {}

        long mapStart = System.currentTimeMillis();

        coordinator.dispatchMapTasks(chunks);

        long reduceStart = System.currentTimeMillis();

        coordinator.waitForFinalResults(numReducers);

        long endTime = System.currentTimeMillis();

        long duration = endTime - startTime;

        System.out.println("Phase Map    : " + (reduceStart - mapStart) + " ms");
        System.out.println("Phase Reduce : " + (endTime - reduceStart) + " ms");
        System.out.println("Temps total  : " + duration + " ms (" + (duration / 1000.0) + " s)");

        coordinator.stopWorkerProcesses();
    }

    public List<String> splitFile(String path, int sizeMB) {
        List<String> files = new ArrayList<>();

        try {
            File f = new File(path);

            BufferedReader br = new BufferedReader(new FileReader(f));

            String line;
            int index = 1;

            long max = sizeMB * 1024L * 1024L;
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

            Process process = new ProcessBuilder(cmd)
                    .inheritIO()
                    .start();

            workerProcesses.add(process);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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
                        global.put(
                                e.getKey(),
                                global.getOrDefault(e.getKey(), 0) + e.getValue()
                        );
                    }

                    received++;

                    String workerName = "REDUCER_" + r.getReducerId();

                    workers.remove(workerName);

                    System.out.println(
                            "Coordinateur : Le Reducer "
                                    + r.getReducerId()
                                    + " a terminé avec succès. Arrêt de la surveillance."
                    );
                }

                s.close();
            }

            try (BufferedWriter w = new BufferedWriter(new FileWriter("final.txt"))) {
                for (Map.Entry<String, Integer> e : global.entrySet()) {
                    w.write(e.getKey() + " -> " + e.getValue());
                    w.newLine();
                }
            }

            System.out.println("DONE");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void stopWorkerProcesses() {
        for (Process process : workerProcesses) {
            process.destroy();

            try {
                if (!process.waitFor(2, java.util.concurrent.TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                }
            } catch (Exception e) {
                process.destroyForcibly();
            }
        }

        System.out.println("Tous les workers ont été arrêtés.");
    }
}