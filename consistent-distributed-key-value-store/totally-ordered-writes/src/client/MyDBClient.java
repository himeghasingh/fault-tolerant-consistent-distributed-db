package client;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import server.SingleServer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MyDBClient extends Client {

    private NodeConfig<String> nodeConfig= null;
    private long counter = 0; // to keep track of unique request IDs
    private Map<Long, Callback> pendingRequests = Collections.synchronizedMap(new HashMap<>());

    public MyDBClient() throws IOException {
    }

    public MyDBClient(NodeConfig<String> nodeConfig) throws IOException {
        super();
        this.nodeConfig = nodeConfig;
    }

    @Override
    protected void handleResponse(byte[] bytes, NIOHeader header) {
        // Extract the identifier from the received message
        String response = new String(bytes);
        long id = 0;
        String actualResponse;
        if (response.contains(":")) {
            id = Long.parseLong(response.split(":")[0]);
            actualResponse = response.split(":")[1];
        } else {
            actualResponse = response;
        }
        // Lookup the callback based on the id
        Callback callback = pendingRequests.remove(id);
        if(callback != null) {
            try {

                callback.handleResponse(actualResponse.getBytes(SingleServer.DEFAULT_ENCODING), header);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void callbackSend(InetSocketAddress isa, String request, Callback callback) throws IOException {
        long currentID = counter++;
        String modifiedRequest = currentID + ":" + request;

        // Store the callback with the request id
        pendingRequests.put(currentID, callback);

        // Send the modified request
        this.send(isa, modifiedRequest);
    }
}
