package server;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.Row;

/**
 * This class should implement your replicated database server. Refer to
 * {@link ReplicatedServer} for a starting point.
 */
public class MyDBReplicatedServer extends MyDBSingleServer {
    private final Cluster cluster;
    private final Session session;
    private final Object lock = new Object();
    private final String myID;
    private final MessageNIOTransport<String, String> serverMessenger;
    private final int THREAD_POOL_SIZE = 100;
    private final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    private final ExecutorService queueConsumerService = Executors.newSingleThreadExecutor();
    private final BlockingQueue<TimestampedMessage> priorityQueue = new PriorityBlockingQueue<>();
    private final AtomicInteger messageSequenceID = new AtomicInteger(0);
    private final String LEADER_ID;
    private Map<Integer, Integer> ackCounts = Collections.synchronizedMap(new HashMap<>());
    private Map<Integer, InetSocketAddress> clientAddresses = Collections.synchronizedMap(new HashMap<>());
    private Map<Integer, Integer> clientMsgIdToSequenceMsgId = Collections.synchronizedMap(new HashMap<>());


    private final int totalServerCount;

    class TimestampedMessage implements Comparable<TimestampedMessage> {
        long timestamp;
        byte[] message;

        public TimestampedMessage(long timestamp, byte[] message) {
            this.timestamp = timestamp;
            this.message = message;
        }

        @Override
        public int compareTo(TimestampedMessage o) {
            return Long.compare(this.timestamp, o.timestamp);
        }
    }

    public MyDBReplicatedServer(NodeConfig<String> nodeConfig, String myID,
                                InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID) - ReplicatedServer
                        .SERVER_PORT_OFFSET), isaDB, myID);
        this.myID = myID;
        this.cluster = Cluster.builder()
                .addContactPoint(isaDB.getHostName())
                .withPort(isaDB.getPort())
                .build();

        this.session = cluster.connect(myID);

        this.serverMessenger = new MessageNIOTransport<String, String>(myID, nodeConfig,
                new AbstractBytePacketDemultiplexer() {
                    @Override
                    public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                        try {
                            return handleIncomingServerMessage(bytes, nioHeader);
                        } catch (InterruptedException | JSONException e) {
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }, true);
        queueConsumerService.submit(this::processMessagesFromQueue);
        LEADER_ID = loadLeaderFromProperties();
        this.totalServerCount = nodeConfig.getNodeIDs().size();
    }

    private String loadLeaderFromProperties() {
        Properties properties = new Properties();
        try (InputStream input = new FileInputStream("conf/servers.properties")) {
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        }

        return Objects.requireNonNull(properties.stringPropertyNames().stream()
                        .filter(name -> name.startsWith("server."))
                        .findFirst()
                        .orElse(null))
                .replace("server.", "");
    }

    private boolean isWriteOperation(String message) {
        String msg = message.trim().toLowerCase();
        return msg.startsWith("insert") || msg.startsWith("update") || msg.startsWith("create") || msg.startsWith("drop") || msg.startsWith("truncate");
    }

    private void sendAckToLeader(int messageId) {
        JSONObject ackJson = new JSONObject();
        try {
            ackJson.put("messageID", messageId);
            ackJson.put("request", "");
            ackJson.put("messageType", "Ack");
            ackJson.put("clientMsgID", 0);
            this.serverMessenger.sendToID(LEADER_ID, ackJson.toString());
        } catch (IOException | JSONException e) {
            e.printStackTrace();
        }
    }

    private void processMessagesFromQueue() {
        long nextExpectedTimestamp = 1;
        while (myID != (LEADER_ID)) {
            try {

                TimestampedMessage msg = priorityQueue.peek();
                if (msg == null) {
                    msg = priorityQueue.take();
                }
                if (msg.timestamp == nextExpectedTimestamp) {
                    String actualRequest = new String(msg.message);
                    priorityQueue.poll();
                    session.execute(actualRequest);
                    nextExpectedTimestamp++;
                    sendAckToLeader((int) msg.timestamp);
                }
            } catch (InterruptedException e) {
                //throw new RuntimeException(e);
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void propagateMultiCastCommit(byte[] bytes, int clientmsgID) {

        synchronized (lock) {
            String actualRequest = new String(bytes);
            int currentMsgID = messageSequenceID.incrementAndGet();
            clientMsgIdToSequenceMsgId.put(currentMsgID, clientmsgID);
            for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
                if (!node.equals(myID)) {
                    try {
                        JSONObject messageJson = new JSONObject();
                        messageJson.put("messageID", currentMsgID);
                        messageJson.put("request", actualRequest);
                        messageJson.put("messageType", "MultiCastCommit");
                        messageJson.put("clientMsgID", clientmsgID);
                        String jsonMessage = messageJson.toString();
                        this.serverMessenger.sendToID(node, jsonMessage);
                    } catch (IOException | JSONException e) {
                        e.printStackTrace();
                    }
                }
            }
            session.execute(actualRequest);
        }
    }

    protected boolean handleIncomingServerMessage(byte[] bytes, NIOHeader header) throws InterruptedException, IOException,JSONException {
        synchronized (lock) {
            String message = new String(bytes);

            JSONObject messageJson = new JSONObject(message);
            int messageID = messageJson.getInt("messageID");
            String actualRequest = messageJson.getString("request");
            String messageType = messageJson.getString("messageType");
            int clientMsgID = messageJson.getInt("clientMsgID");
            switch (messageType) {
                case "MultiCastCommit":
                    try {
                        priorityQueue.put(new TimestampedMessage(messageID, actualRequest.getBytes()));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    break;

                case "MultiCastRequest":
                    propagateMultiCastCommit(actualRequest.getBytes(), clientMsgID);
                    break;

                case "Ack":
                    processAck(messageID);
                    break;
                default:
                    break;
            }
        }
        return false;
    }

    private void processAck(int messageId) throws IOException {
        ackCounts.merge(messageId, 1, Integer::sum);
        if (ackCounts.get(messageId) == totalServerCount - 1) {
            // All ACKs received, send response to client
            ackCounts.remove(messageId);
            int clientmsgID = clientMsgIdToSequenceMsgId.get(messageId);
            InetSocketAddress clientAddress = clientAddresses.get(clientmsgID);
            String modifiedResponse = clientmsgID + ":" + "Successfully execuited";
            this.clientMessenger.send(clientAddress, modifiedResponse.getBytes());
        }
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        int clientMsgId = 0;
        try {
            String request = new String(bytes);
            if (request.contains(":")) {
                clientMsgId = Integer.parseInt((request.split(":")[0]));
                request = request.split(":")[1];
            }
            clientAddresses.put(clientMsgId, header.sndr);
            if (isWriteOperation(request)) {
                if (myID.equals(LEADER_ID)) {
                    propagateMultiCastCommit(request.getBytes(), clientMsgId);
                } else {
                    try {
                        JSONObject messageJson = new JSONObject();
                        messageJson.put("messageID", messageSequenceID.get());
                        messageJson.put("request", request);
                        messageJson.put("messageType", "MultiCastRequest");
                        messageJson.put("clientMsgID", clientMsgId);
                        String jsonMessage = messageJson.toString();
                        this.serverMessenger.sendToID(LEADER_ID, jsonMessage);
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }



                }
            } else {
                ResultSet resultSet = session.execute(request);
                String resultString = StreamSupport.stream(resultSet.spliterator(), false)
                        .map(Row::toString)
                        .collect(Collectors.joining("\n"));

                String modifiedResponse = clientMsgId + ":" + resultString;
                this.clientMessenger.send(header.sndr, modifiedResponse.getBytes());
                //SEND RESPONSE BACK
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        // Stop the serverMessenger
        this.serverMessenger.stop();

        // Close the session and cluster
        if (session != null) {
            session.close();
        }
        if (cluster != null) {
            cluster.close();
        }
        // Shutdown the executorService
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("ThreadPool did not terminate");
            }
        } catch (InterruptedException ie) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        // Shutdown the queueConsumerService
        queueConsumerService.shutdown();
        try {
            if (!queueConsumerService.awaitTermination(60, TimeUnit.SECONDS)) {
                queueConsumerService.shutdownNow();
            }
        } catch (InterruptedException ie) {
            queueConsumerService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        // Call the close of the superclass
        super.close();
    }
}
