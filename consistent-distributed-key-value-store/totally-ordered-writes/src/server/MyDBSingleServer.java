package server;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.nioutils.NIOHeader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MyDBSingleServer extends SingleServer {

    private final Cluster cluster;
    private final Session session;

    public MyDBSingleServer(InetSocketAddress isa, InetSocketAddress isaDB,
                            String keyspace) throws IOException {
        super(isa, isaDB, keyspace);

        this.cluster = Cluster.builder()
                .addContactPoint(isaDB.getHostName())
                .withPort(isaDB.getPort())
                .build();

        this.session = cluster.connect(keyspace);
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String request = new String(bytes);
        String modifiedResponse;
        long currentID = 0;
        try {
            if (request.contains(":")) {
                currentID = Long.parseLong(request.split(":")[0]);
                String actualRequest = request.split(":")[1];
                ResultSet resultSet = session.execute(actualRequest);

                String responseToSend = "Operation completed successfully.";
                modifiedResponse = String.valueOf(currentID)+ ":" + responseToSend;
            } else {
                ResultSet resultSet = session.execute(request);
                modifiedResponse =  "Operation completed successfully.";
            }

        } catch (Exception e) {
            modifiedResponse = currentID + ":" + "Error executing operation: " + e.getMessage();
        }

        try {

            this.clientMessenger.send(header.sndr, modifiedResponse.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if(session != null) {
            session.close();
        }
        if(cluster != null) {
            cluster.close();
        }
        super.close();
    }
}
