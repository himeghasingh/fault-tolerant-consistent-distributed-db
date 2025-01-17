package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class should implement your {@link Replicable} database app if you wish
 * to use Gigapaxos.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) A {@link Replicable} must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos. However, in this assignment, we need myID as
 * each replica uses a different keyspace (because we are pretending different
 * replicas are like different keyspaces), so we use myID only for initiating
 * the connection to the backend data store.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class MyDBReplicableAppGP implements Replicable {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will use
	 * this value provided it is no greater than its MAX_SLEEP limit. Faster is not
	 * necessarily better, so don't sweat speed. Focus on safety.
	 */
	public static final int SLEEP = 1000;
	private Session session;
	private final Cluster cluster;

	/**
	 * All Gigapaxos apps must either support a no-args constructor or a constructor
	 * taking a String[] as the only argument. Gigapaxos relies on adherence to this
	 * policy in order to be able to reflectively construct customer application
	 * instances.
	 *
	 * @param args Singleton array whose args[0] specifies the keyspace in the
	 *             backend data store to which this server must connect. Optional
	 *             args[1] and args[2]
	 * @throws IOException
	 */
	public MyDBReplicableAppGP(String[] args) throws IOException {
		// TODO: setup connection to the data store and keyspace
		// throw new RuntimeException("Not yet implemented");
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect(args[0]);
	}

	/**
	 * Refer documentation of {@link Replicable#execute(Request, boolean)} to
	 * understand what the boolean flag means.
	 * <p>
	 * You can assume that all requests will be of type
	 * {@link edu.umass.cs.gigapaxos.paxospackets.RequestPacket}.
	 *
	 * @param request
	 * @param b
	 * @return
	 */
	@Override
	public boolean execute(Request request, boolean b) {
		// TODO: submit request to data store
		// throw new RuntimeException("Not yet implemented");
		// If the request received is a valid object of RequestPacket class, we execute
		// the request
		if (RequestPacket.class.isInstance(request)) {
			try {
				session.execute(((RequestPacket) request).requestValue);
				return true;
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Execution of request failed due to error " + e.getMessage());
			}
		} else {
			System.err.println("Invalid Request");
		}
		return false;
	}

	/**
	 * Refer documentation of
	 * {@link edu.umass.cs.gigapaxos.interfaces.Application#execute(Request)}
	 *
	 * @param request
	 * @return
	 */
	@Override
	public boolean execute(Request request) {
		// TODO: execute the request by sending it to the data store
		// throw new RuntimeException("Not yet implemented");
		// false is sent to the variable b as it serves as a flag for doNotReplyToClient
		// and we want the client to receive a response
		return this.execute(request, false);
	}

	/**
	 * Refer documentation of {@link Replicable#checkpoint(String)}.
	 *
	 * @param s
	 * @return
	 */
	@Override
	public String checkpoint(String s) {
		// TODO:
		// throw new RuntimeException("Not yet implemented");
		StringBuilder eventsHistory = new StringBuilder();
		// All the data from grade table is saved into resultSet
		ResultSet resultSet = session.execute("SELECT * FROM grade");
		// resultSet is parsed row wise and events are stored in the format
		// eventID:eventList; in eventsHistory
		// This eventsHistory serves as the checkpoint to store the state
		for (Row row : resultSet) {
			int id = row.getInt("id");
			List<Integer> eventsList = row.getList("events", Integer.class);
			eventsHistory.append(id).append(":").append(eventsList.toString()).append(";");
		}
		return eventsHistory.toString();
	}

	/**
	 * Refer documentation of {@link Replicable#restore(String, String)}
	 *
	 * @param s
	 * @param s1
	 * @return
	 */
	@Override
	public boolean restore(String s, String s1) {
		// TODO:
		// throw new RuntimeException("Not yet implemented");
		// s1 contains the state, which we parse using delimiters assigned before and
		// form a list of events and insert those events back into the table
		if (!s1.equals("{}") && !s1.isEmpty()) {
			try {
				String[] dataEntries = s1.split(";");
				for (String dataEntry : dataEntries) {
					String[] entryParts = dataEntry.split(":");
					if (entryParts.length > 1) {
						int id = entryParts[0].isEmpty() ? 0 : Integer.parseInt(entryParts[0]);
						List<Integer> eventsList = retrieveEvents(entryParts[1]);
						String insertQuery = String.format("INSERT INTO grade (id, events) VALUES (%d, %s)", id,
								eventsList.toString());
						session.execute(insertQuery);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Restore failed due to error " + e.getMessage());
				return false;
			}
		}
		return true;
	}

	/**
	 * No request types other than {@link edu.umass.cs.gigapaxos.paxospackets
	 * .RequestPacket will be used by Grader, so you don't need to implement this
	 * method.}
	 *
	 * @param s
	 * @return
	 * @throws RequestParseException
	 */
	@Override
	public Request getRequest(String s) throws RequestParseException {
		return null;
	}

	/**
	 * @return Return all integer packet types used by this application. For an
	 *         example of how to define your own IntegerPacketType enum, refer
	 *         {@link edu.umass.cs.reconfiguration.examples.AppRequest}. This method
	 *         does not need to be implemented because the assignment Grader will
	 *         only use {@link edu.umass.cs.gigapaxos.paxospackets.RequestPacket}
	 *         packets.
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>();
	}

	/**
	 * This method takes the string of events as input from the state string and
	 * formats it into a list of events
	 *
	 * @param events
	 * @return
	 */

	public List<Integer> retrieveEvents(String events) {

		if (!events.equals("{}") && !events.isEmpty()) {
			List<Integer> formattedEventsList = new ArrayList<>();
			//First and Last elements containing brackets are removed and "," delimiter is used to demarcate events
			String[] eventsList = events.substring(1, events.length() - 1).split(",");
			for (String event : eventsList) {
				formattedEventsList.add(event.trim().isEmpty() ? 0 : Integer.parseInt(event.trim()));
			}
			return formattedEventsList;
		} else {
			//If events is empty, an empty list is returned, signifying no insert in required
			return Collections.emptyList();
		}
	}

	public void close() {
		// Close Cassandra session
		if (session != null) {
			try {
				session.close();
			} catch (Exception e) {
				System.err.println("Error closing Cassandra session: " + e.getMessage());
			}
		}
		// Close Cassandra cluster
		if (cluster != null) {
			try {
				cluster.close();
			} catch (Exception e) {
				System.err.println(" Error closing Cassandra cluster: " + e.getMessage());
			}
		}
	}

}
