package Reduce;
import common.FinalResult;
import common.HeartbeatData;
import common.IntermediateData;
import common.Message;
import common.MessageType;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

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

        // Au lieu d'envoyer au coordinateur, on sauvegarde dans un fichier
        saveResultsToFile();
        running = false; // stoppe le heartbeat thread
        //  ET on envoie le résultat final au coordinateur ! (AJOUTE CETTE LIGNE)
        sendFinalResultToCoordinator();

    }


    private void saveResultsToFile() {
        // Le nom du fichier contiendra l'ID du Reducer pour ne pas écraser les autres
        String fileName = "resultat_reducer_" + reducerId + ".txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            // On parcourt le dictionnaire final et on écrit chaque mot et son compte
            for (Map.Entry<String, Integer> entry : finalCounts.entrySet()) {
                writer.write(entry.getKey() + " -> " + entry.getValue());
                writer.newLine(); // Retour à la ligne
            }
            System.out.println("Resultats sauvegardes dans le fichier : " + fileName);
        } catch (IOException e) {
            System.err.println("Erreur lors de l'ecriture du fichier !");
            e.printStackTrace();
        }
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

    private volatile boolean running = true; // flag
    private void startHeartbeatThread() {
        Thread heartbeatThread = new Thread(() -> {
            while (running) { // ✅ vérifie le flag
                try (
                        Socket socket = new Socket("localhost", 7001);
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

                    HeartbeatData hb = new HeartbeatData("REDUCER", reducerId);
                    out.writeObject(new Message(MessageType.HEARTBEAT, hb));
                } catch (Exception e) {
                    // silence
                }
                try { Thread.sleep(3000); } catch (Exception ignored) {} // ✅ sleep DEHORS du try socket
            }
        });
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }
    public static void main(String[] args) throws IOException {
        int reducerId = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);
        int expectedMaps = Integer.parseInt(args[2]);

        ReduceWorker worker = new ReduceWorker(reducerId, port, expectedMaps);
        worker.startHeartbeatThread();
        worker.start();
    }

    private void sendFinalResultToCoordinator() {
        try (
                Socket socket = new Socket("localhost", 7000);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

            FinalResult result = new FinalResult(reducerId, finalCounts);
            out.writeObject(new Message(MessageType.FINAL_RESULT, result));
            out.flush();
            System.out.println("ReduceWorker " + reducerId + " sent final result to Coordinator");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}