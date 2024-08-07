package server;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.nioutils.NIOHeader;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MyDBSingleServer extends SingleServer {
	private String keyspace;
	private Session session;

	/**
	 * This class should implement the logic necessary to perform the requested
	 * operation on the database and return the response back to the client.
	 */
	public MyDBSingleServer(InetSocketAddress isa, InetSocketAddress isaDB, String keyspace) throws IOException {
		super(isa, isaDB, keyspace);
		this.keyspace = keyspace;
		this.session = Cluster.builder().addContactPoint(isaDB.getHostName()).build().connect(keyspace);
	}

	// Receives and processes the client request and sends a response
	@Override
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
		try {
			long requestId = -1;
			String requestMessage, response = null;
			boolean asynchronous = false;
			String request = new String(bytes, SingleServer.DEFAULT_ENCODING);
			// Checks if request is asynchronous, if yes, extracts the request and requestID
			if (request.contains(":")) {
				String[] parts = request.split(":", 2);
				requestId = Long.parseLong(parts[0]);
				requestMessage = parts[1];
				asynchronous = true;
			} else {
				requestMessage = request;
			}
			// Executes the request on the Cassandra database
			ResultSet resultSet = session.execute(requestMessage);

			// Constructs the response
			StringBuilder responseBuilder = new StringBuilder();
			for (com.datastax.driver.core.Row row : resultSet) {
				responseBuilder.append(row.toString()).append(" ");
			}
			response = responseBuilder.toString();
			// Appends the requestID to the response in case of an asynchronous request
			if (asynchronous) {
				response = requestId + ":" + response;
			}
			// Sends the response to the client
			this.clientMessenger.send(header.sndr, response.getBytes(SingleServer.DEFAULT_ENCODING));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		super.close();
		// Closes the session gracefully
		if (session != null) {
			session.close();

		}
	}
}
