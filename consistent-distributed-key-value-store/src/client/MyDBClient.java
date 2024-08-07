package client;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import server.SingleServer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;

public class MyDBClient extends Client {

	private NodeConfig<String> nodeConfig = null;
	private Map<Long, Callback> callbackMap = Collections.synchronizedMap(new HashMap<>());
	private long reqnum = 0;

	public MyDBClient() throws IOException {
	}

	public MyDBClient(NodeConfig<String> nodeConfig) throws IOException {
		super();
		this.nodeConfig = nodeConfig;
		this.callbackMap = new HashMap<>();
	}

	// Sends a request to the server and is responsible for the asynchronous callback
	@Override
	public void callbackSend(InetSocketAddress isa, String request, Callback callback) throws IOException {
		long requestId = enqueueRequest(request);
		callbackMap.put(requestId, callback);
		String requestWithId = requestId + ":" + request;
		super.send(isa, requestWithId);
	}

	// Handles the server response
	@Override
	protected void handleResponse(byte[] bytes, NIOHeader header) {
		try {
			String response = new String(bytes, SingleServer.DEFAULT_ENCODING);
			// Extracts the requestID from the server response
			long requestId = extractRequestIdFromResponse(response);
			// Fetches the corresponding callback for the respective requestID key
			Callback callback = callbackMap.get(requestId);

			// If a callback is available, invokes the callback method in parent class and
			// removes the entry from the map
			if (callback != null) {
				callback.handleResponse(bytes, header);
				callbackMap.remove(requestId);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Extracts the requestID from the response, and returns -1 if no requestID is
	// found
	private long extractRequestIdFromResponse(String response) {
		String[] parts = response.split(":", 2);
		if (parts.length > 0) {
			try {
				return Long.parseLong(parts[0]);
			} catch (NumberFormatException e) {

			}
		}
		return -1;
	}

	// Maintains a counter to generate the requestID
	private synchronized long enqueueRequest(String request) {
		return reqnum++;
	}
}
