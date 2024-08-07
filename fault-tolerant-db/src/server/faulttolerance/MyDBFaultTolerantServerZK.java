package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import org.apache.zookeeper.*;
import server.ReplicatedServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class should implement your replicated fault-tolerant database server if
 * you wish to use Zookeeper or other custom consensus protocols to order client
 * requests.
 * <p>
 * Refer to {@link server.ReplicatedServer} for a starting point for how to do
 * server-server messaging or to {@link server.AVDBReplicatedServer} for a
 * non-fault-tolerant replicated server.
 * <p>
 * You can assume that a single *fault-tolerant* Zookeeper server at the default
 * host:port of localhost:2181 and you can use this service as you please in the
 * implementation of this class.
 * <p>
 * Make sure that both a single instance of Cassandra and a single Zookeeper
 * server are running on their default ports before testing.
 * <p>
 * You can not store in-memory information about request logs for more than
 * {@link #MAX_LOG_SIZE} requests.
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer implements Watcher {

    /**
     * Set this value to as small a value with which you can get tests to still
     * pass. The lower it is, the faster your implementation is. Grader* will
     * use this value provided it is no greater than its MAX_SLEEP limit.
     */
    public static final int SLEEP = 1000;
    private BlockingQueue<String> messageQueue;
    private static final String DATA_ZNODE = "/grade_data";
    private Set<String> knownChildren = new HashSet<>();
    private final Object eventQueueLock = new Object();
    private final Object dataChangeLock = new Object();


    /**
     * Set this to true if you want all tables drpped at the end of each run
     * of tests by GraderFaultTolerance.
     */
    public static final boolean DROP_TABLES_AFTER_TESTS = true;


    /**
     * Maximum permitted size of any collection that is used to maintain
     * request-specific state, i.e., you can not maintain state for more than
     * MAX_LOG_SIZE requests (in memory or on disk). This constraint exists to
     * ensure that your logs don't grow unbounded, which forces
     * checkpointing to
     * be implemented.
     */
    public static final int MAX_LOG_SIZE = 400;

    public static final int DEFAULT_PORT = 2181;
    private BlockingQueue<WatchedEvent> eventQueue = new LinkedBlockingQueue<>();
    private final Cluster cluster;
    private Session session;
    private ZooKeeper zk;
    private String myID;
    private final Object updateLock = new Object();

    int index = 0;
    private ExecutorService executorService;

    /**
     * @param nodeConfig Server name/address configuration information read
     *                   from
     *                   conf/servers.properties.
     * @param myID       The name of the keyspace to connect to, also the name
     *                   of the server itself. You can not connect to any other
     *                   keyspace if using Zookeeper.
     * @param isaDB      The socket address of the backend datastore to which
     *                   you need to establish a session.
     * @throws IOException
     */
    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String
            myID, InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID) - ReplicatedServer
                        .SERVER_PORT_OFFSET), isaDB, myID);
        messageQueue = new LinkedBlockingQueue<>();

        this.myID = myID; // example server2 isaDB cassandra localhost/127.0.0.1:9042
        this.cluster = Cluster.builder().addContactPoint(isaDB.getHostName()).withPort(isaDB.getPort()).build();
        this.session = cluster.connect(myID); // Connect to the keyspace named after the server ID
        try {
            this.zk = new ZooKeeper("localhost:2181", 30000, this);
            //System.out.println(myID + " Established a zookeeper server");
            zk.exists(DATA_ZNODE, true);
            initDataZnode();
            zk.getChildren(DATA_ZNODE, true);
            Thread queueProcessingThread = new Thread(() -> {
                try {
                    this.processMessagesFromQueue();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            queueProcessingThread.start();

            Thread eventProcessingThread = new Thread(() -> {
                try {
                    this.processEvents();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });

            eventProcessingThread.start();
            this.executorService = Executors.newFixedThreadPool(100); // Adjust the number of threads as needed
            // Handle additional setup or recovery tasks
        } catch (InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }
        // TODO: Make sure to do any needed crash recovery here.
    }

    private void initDataZnode() {
        try {
            if (zk.exists(DATA_ZNODE, false) == null) {
                zk.create(DATA_ZNODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                //System.out.println(myID + " Created a new ZNODE");
                zk.getData(DATA_ZNODE, true, null);
            } else {
                zk.getData(DATA_ZNODE, true, null);
                //System.out.println(myID + " Node already exists, initiating crash recovery");
                recoverFromCrash();
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void recoverFromCrash() throws KeeperException, InterruptedException {
        // List all child znodes under /grade_data
        Set<String> idsInCassandra = new HashSet<>();
        List<String> children = zk.getChildren(DATA_ZNODE, false);
        String query = String.format("SELECT id FROM grade;");
        ResultSet resultSet = session.execute(query);
        for (Row row : resultSet) {
            idsInCassandra.add(String.valueOf(row.getInt("id")));
        }
        for (String child : children) {
            String childPath = DATA_ZNODE + "/" + child;
            byte[] data = zk.getData(childPath, false, null);
            // Assuming the data is directly usable for Cassandra update
            String event = new String(data);
            if (!event.isEmpty()) {
                String reconstructedSQL = String.format("update grade SET events=events+%s where id=%s;", event, child);
                session.execute(reconstructedSQL);  // Execute the update in Cassandra
                //System.out.println(myID + " Recovering data for znode: " + reconstructedSQL + "  Child:" + child + " Size  ");
            } else {
                //System.out.println(myID + " Skipping update for existing event: " + event + " for ID: " + child);
            }
        }
        //System.out.println(myID + " Crash recovery completed");
    }


    /**
     * TODO: process bytes received from clients here.
     */
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String request = new String(bytes);
        messageQueue.offer(request);
    }

    private void processMessagesFromQueue() throws InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            String request = messageQueue.take(); // Retrieves and removes the head of the queue
            //System.out.println(myID + " QUEUE " + request);
            if (request != null) {
                try {
                    if (request.toLowerCase().startsWith("insert")) {
                        handleInsertRequest(request);
                        session.execute(request);
                        //System.out.println(myID + "Inserted the  request on  cassandra" + request);
                    } else if (request.toLowerCase().startsWith("update")) {
                        handleUpdateRequest(request);
                        //System.out.println(myID + " " + request);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break; // Exit the loop
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void handleUpdateRequest(String request) throws KeeperException, InterruptedException {
        // Extract the ID and events from the request and update the znode data
        int id = extractIdFromUpdate(request);
        String idZnodePath = DATA_ZNODE + "/" + id;
        String lockPath = idZnodePath + "_lock"; // Path for the lock node

        while (true) {
            if (acquireLock(id)) {
                try {
                    // Perform the update
                    if (zk.exists(idZnodePath, false) != null) {
                        byte[] currentData = zk.getData(idZnodePath, false, null);
                        String dataStr = new String(currentData);
                        byte[] updatedData = updateEventData(dataStr, request);
                        String updatedataStr = new String(updatedData);
                        zk.setData(idZnodePath, updatedData, -1); // -1 to ignore the version check
                        break; // Exit the loop after successful update
                    } else {
                        System.err.println(myID + "Znode does not exist: " + idZnodePath);
                    }
                    break;
                } catch (KeeperException e) {
                    // Handle ZooKeeper exceptions
                    System.err.println(myID + "ZooKeeper operation failed: " + e.getMessage() + "   REQUEST  " + request + " ID  " + id);
                    // Depending on the exception, decide whether to retry or abort
                } catch (InterruptedException e) {
                    // Handle thread interruption
                    Thread.currentThread().interrupt();
                    System.err.println(myID + "Thread operation failed: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    releaseLock(id); // Ensure lock is always released
                }
            } else {
                // Lock not acquired, set a watcher and wait
                LockWatcher lockWatcher = new LockWatcher();
                if (zk.exists(lockPath, lockWatcher) != null) {
                    lockWatcher.await(); // Wait for the lock to be released
                }
            }
        }
    }

    private boolean acquireLock(int id) throws KeeperException, InterruptedException {
        String lockPath = DATA_ZNODE + "/" + id + "_lock";
        try {
            zk.create(lockPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            return true; // Lock acquired
        } catch (KeeperException.NodeExistsException e) {
            return false; // Lock is already held by another process
        }
    }

    private void releaseLock(int id) throws KeeperException, InterruptedException {
        String lockPath = DATA_ZNODE + "/" + id + "_lock";
        try {
            zk.delete(lockPath, -1); // -1 to match any node version
        } catch (KeeperException.NoNodeException e) {
            //System.out.println("The node doesn't exist, it may have been already released or expired");
        }
    }


    private byte[] updateEventData(String currentData, String request) {
        // Implement logic to update the event data array with the new event from the request
        // Placeholder example
        currentData = currentData.replace("[", "").replace("]", "");
        if (currentData.isEmpty()) {
            currentData = "[" + extractEventFromUpdate(request) + "]";
        } else {
            currentData = "[" + currentData + ", " + extractEventFromUpdate(request) + "]";
        }
        return (currentData).getBytes();
    }

    private int extractEventFromUpdate(String request) {
        // This pattern matches the numeric event value in the request string
        String pattern = "update grade SET events=events\\+\\[(\\d+)\\] where id=(-?\\d+);";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(request);
        if (m.find()) {
            return Integer.parseInt(m.group(1));
        } else {
            throw new IllegalArgumentException("Invalid update request format");
        }
    }

    private void handleInsertRequest(String request) throws KeeperException, InterruptedException {
        // Extract the ID from the request and create a child znode for it
        int id = extractIdFromInsert(request);
        String idZnodePath = DATA_ZNODE + "/" + id;
        if (zk.exists(idZnodePath, false) == null) {
            zk.create(idZnodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.exists(idZnodePath, true);
            //System.out.println(myID + " Created a Znode(KEY) at this Path" + idZnodePath);
        } else {
            //System.out.println(myID + " Not creating Znode(KEY) As it alreay Exists!!");
        }
    }

    private int extractIdFromUpdate(String request) {
        // This pattern matches the numeric ID value in the request string
        String pattern = "update grade SET events=events\\+\\[(\\d+)\\] where id=(-?\\d+);";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(request);
        if (m.find()) {
            return Integer.parseInt(m.group(2));
        } else {
            throw new IllegalArgumentException("Invalid update request format");
        }
    }

    private int extractIdFromInsert(String request) {
        // This pattern matches the numeric value in the request string
        String pattern = "insert into grade \\(id, events\\) values \\((-?\\d+), \\[\\]\\);";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(request);
        if (m.find()) {
            return Integer.parseInt(m.group(1));
        } else {
            throw new IllegalArgumentException("Invalid insert request format");
        }
    }

    /**
     * TODO: process bytes received from fellow servers here.
     */
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        throw new RuntimeException("Not implemented");
    }


    /**
     * TODO: Gracefully close any threads or messengers you created.
     */
    @Override
    public void close() {
        // Close ZooKeeper client
        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                System.err.println(myID + " Error closing ZooKeeper client: " + e.getMessage());
            }
        }
		// Close Cassandra session
        if (session != null) {
            try {
                session.close();
            } catch (Exception e) {
                System.err.println(myID + " Error closing Cassandra session: " + e.getMessage());
            }
        }
		// Close Cassandra cluster
        if (cluster != null) {
            try {
                cluster.close();
            } catch (Exception e) {
                System.err.println(myID + " Error closing Cassandra cluster: " + e.getMessage());
            }
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
		// Close any additional resources if necessary
		super.close(); // Call close method of the superclass if it performs important cleanup
    }


    @Override
    public void process(WatchedEvent event) {
        if (event.getPath() != null && event.getPath().endsWith("_lock")) {
            //System.out.println(myID + " Ignoring _lock node change event");
        } else if (executorService != null) {
            executorService.submit(() -> handleEvent(event));
        } else {
            //System.out.println("Handle the case where executorService is null");
        }

    }
	private void handleEvent(WatchedEvent event) {
		synchronized (eventQueueLock) {
            eventQueue.offer(event);
        }
    }
	private void processEvents() throws InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            WatchedEvent event = eventQueue.take();
            if (event.getType() == Event.EventType.NodeChildrenChanged && event.getPath().equals(DATA_ZNODE)) {
                List<String> currentChildren = null;
                try {
                    currentChildren = zk.getChildren(DATA_ZNODE, true);
                } catch (KeeperException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                for (String child : currentChildren) {
                    if (!knownChildren.contains(child) && !child.endsWith("_lock")) {
                        // This is a new child node
                        String childPath = DATA_ZNODE + "/" + child;
                        try {
                            zk.exists(childPath, true); // Set a watch on the new child node
                            index = 0;
                            //UpdateIndex(child);
                        } catch (KeeperException | InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        knownChildren.add(child); // Add to known children list
                        //System.out.println(myID + " New child node detected and watched: " + childPath);
                    }
                }
            }
            // Check if the event is a NodeDataChanged event
            if (event.getType() == Event.EventType.NodeDataChanged) {
                synchronized (dataChangeLock) {
                    String idZnodePath = event.getPath();
                    int id = extractIdFromZnodePath(idZnodePath);
                    // Get data from the znode and update Cassandra
                    try {
                        byte[] newData = zk.getData(event.getPath(), true, null);
                        String dataStr = new String(newData);
                        dataStr = dataStr.replace("[", "").replace("]", ""); // Remove square brackets
                        String[] numberStrings = dataStr.split(",\\s*"); // Split by comma and optional whitespace
                        int[] numbers = new int[numberStrings.length]; // Array to store the parsed integers
                        for (int i = 0; i < numberStrings.length; i++) {
                            numbers[i] = Integer.parseInt(numberStrings[i].trim()); // Convert to integer and store in array
                        }
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append("[");
                        UpdateIndex(String.valueOf(id));
                        for (int i = index; i < numbers.length; i++) {
							stringBuilder.append(numbers[i]);
                            if (i < numbers.length - 1) {
                                stringBuilder.append(", ");
                            }
                            index++;
                        }
                        stringBuilder.append("]");
                        String numberString = stringBuilder.toString();
                        String query = String.format("SELECT id FROM grade WHERE id = %d;", id);
                        ResultSet rs = session.execute(query);
                        if (rs.one() == null) {
                            String reconstructedSQL0 = String.format("insert into grade (id, events) values (%d, []);", id);
                            rs = session.execute(reconstructedSQL0);  // Execute the reconstructed query in Cassandra
                        }
						String reconstructedSQL = String.format("update grade SET events = events + %s where id = %d;", numberString, id);
                        try {
                            ResultSet rs1 = session.execute(reconstructedSQL);// Execute the reconstructed query in Cassandra
                            String query2 = String.format("SELECT * FROM grade WHERE id = %d;", id);
                            ResultSet rs2 = session.execute(query2);
                            //System.out.println(myID + "  " + String.format("update grade SET events = events + %s where id = %d;", numberString, id) + "  ---------------------------------->" + rs2.one());
                        } catch (Exception e) {
                            // Handle exceptions - this means the update failed
                            System.err.println("Update query failed: " + e.getMessage());
                        }
						zk.exists(event.getPath(), true);// Reset the watch
					} catch (InterruptedException | KeeperException e) {
                        Thread.currentThread().interrupt(); // restore interrupted status
                        System.err.println("Thread interrupted: " + e.getMessage());
                        break; // optional, depending on if you want to terminate on interruption
                    }
                }
            }
        }
    }
	private void UpdateIndex(String id) {
        String checkKeyExistQuery = String.format("SELECT id FROM grade WHERE id = %s;", id);
        ResultSet rsCheck = session.execute(checkKeyExistQuery);
        if (rsCheck.one() != null) {
            // Key exists, get the size of the events list in Cassandra
            String getEventsQuery = String.format("SELECT events FROM grade WHERE id = %s;", id);
            ResultSet rsEvents = session.execute(getEventsQuery);
            Row row = rsEvents.one();
            if (row != null) {
                List<Integer> eventsList = row.getList("events", Integer.class);
                index = eventsList.size(); // Set index to the size of the events list
            } else {
                index = 0; // No events list found, set index to 0
            }
        } else {
            // Key does not exist, set index to 0
            index = 0;
        }
    }

    private int extractIdFromZnodePath(String znodePath) {
        // Assuming znodePath is in the format "/grade_data/<id>"
        String[] parts = znodePath.split("/");
        if (parts.length > 2) {
            try {
                return Integer.parseInt(parts[2]);  // Extract the ID part
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid znode path format");
            }
        } else {
            throw new IllegalArgumentException("Invalid znode path format");
        }
    }
	/**
     * @param args args[0] must be server.properties file and args[1] must be
     *             myID. The server prefix in the properties file must be
     *             ReplicatedServer.SERVER_PREFIX. Optional args[2] if
     *             specified
     *             will be a socket address for the backend datastore.
     * @throws IOException
     */
	private class LockWatcher implements Watcher {
        private final CountDownLatch latch = new CountDownLatch(1);
		@Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDeleted) {
                latch.countDown(); // Release the latch if the lock node is deleted
            }
        }
		public void await() throws InterruptedException {
            latch.await(); // Wait until the latch is released
        }
    }
	public static void main(String[] args) throws IOException {
        new MyDBFaultTolerantServerZK(NodeConfigUtils.getNodeConfigFromFile
                (args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer
                        .SERVER_PORT_OFFSET), args[1], args.length > 2 ? Util
                .getInetSocketAddressFromString(args[2]) : new
                InetSocketAddress("localhost", 9042));
    }

}