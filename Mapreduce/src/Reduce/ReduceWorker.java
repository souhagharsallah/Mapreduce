package Reduce;
import common.FinalResult;
import common.IntermediateData;
import common.Message;
import common.MessageType;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.io.ObjectOutputStream;

public class ReduceWorker {
    private int reducerId;
    private int listenPort;
    private int expectedMaps;
    private int receivedMaps = 0;

    private final Map<String, Integer> finalCounts = new HashMap<>();

    public ReduceWorker(int reducerId, int listenPort, int expectedMaps) {
        this.reducerId = reducerId;
        this.listenPort = listenPort;
        this.expectedMaps = expectedMaps;
    }

    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(listenPort);
        System.out.println("ReduceWorker " + reducerId + " listening on port " + listenPort);

        while (receivedMaps < expectedMaps) {
            Socket socket = serverSocket.accept();
            handleConnection(socket);
        }

        System.out.println("ReduceWorker " + reducerId + " finished aggregation.");
        printFinalCounts();
        sendFinalResultToCoordinator();
    }

    private void handleConnection(Socket socket) {
        try (
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())
        ) {
            Message msg = (Message) in.readObject();

            if (msg.getType() == MessageType.INTERMEDIATE_DATA) {
                IntermediateData data = (IntermediateData) msg.getPayload();

                for (Map.Entry<String, Integer> entry : data.getWordCounts().entrySet()) {
                    String word = entry.getKey();
                    int count = entry.getValue();

                    finalCounts.put(word, finalCounts.getOrDefault(word, 0) + count);
                }

                receivedMaps++;
                System.out.println("ReduceWorker " + reducerId +
                        " received data from MapWorker " + data.getMapId() +
                        " (" + receivedMaps + "/" + expectedMaps + ")");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void printFinalCounts() {
        System.out.println("=== Final counts for Reducer " + reducerId + " ===");
        for (Map.Entry<String, Integer> entry : finalCounts.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
    }

    public static void main(String[] args) throws IOException {
        int reducerId = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);
        int expectedMaps = Integer.parseInt(args[2]);

        ReduceWorker worker = new ReduceWorker(reducerId, port, expectedMaps);
        worker.start();
    }

    private void sendFinalResultToCoordinator() {
        try (
                Socket socket = new Socket("localhost", 7000);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())
        ) {
            FinalResult result = new FinalResult(reducerId, finalCounts);

            out.writeObject(new Message(MessageType.FINAL_RESULT, result));
            out.flush();

            System.out.println("ReduceWorker " + reducerId + " sent final result to Coordinator");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
