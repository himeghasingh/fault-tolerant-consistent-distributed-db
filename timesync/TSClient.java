package timesync;
import java.net.*;
import java.nio.ByteBuffer;

public class TSClient {
    public static void main(String[] args) {
        String serverHost = "127.0.0.1"; // Default server host
        int serverPort = 15750;

        if (args.length > 0) {
            serverHost = args[0];
            
        }

        if (args.length > 1) {
            serverPort = Integer.parseInt(args[1]);
        }

        try {
            DatagramSocket socket = new DatagramSocket();
            InetAddress serverAddress = InetAddress.getByName(serverHost);

            ByteBuffer requestBuffer = ByteBuffer.allocate(48);
            //Flag to specify NTP protocol
            requestBuffer.put((byte) 0b00100011); 

            long clientTransmitTime = System.currentTimeMillis();
            //As timestamp is 8 bytes long, inserting at position 40
            requestBuffer.putLong(40, clientTransmitTime);

            DatagramPacket requestPacket = new DatagramPacket(requestBuffer.array(), requestBuffer.array().length, serverAddress, serverPort);

            // Send request
            socket.send(requestPacket);

            byte[] responseData = new byte[48];
            DatagramPacket responsePacket = new DatagramPacket(responseData, responseData.length);

            // Receive response
            socket.receive(responsePacket);
            
            ByteBuffer responseBuffer = ByteBuffer.wrap(responsePacket.getData());
            // Server Receive Timestamp
            long serverReceiveTime = responseBuffer.getLong();

            //Server Transmit Timestamp
            long serverTransmitTime = responseBuffer.getLong(); 

            long clientReceiveTime = System.currentTimeMillis();
            long rtt = clientReceiveTime - clientTransmitTime;
            long offset = ((serverReceiveTime - clientTransmitTime) + (serverTransmitTime - clientReceiveTime)) / 2;
            //Calculating synchronized time
            long synchronizedTime = serverTransmitTime + offset;
            System.out.println("REMOTE_TIME " + serverTransmitTime);
            System.out.println("LOCAL_TIME " + clientTransmitTime);
            System.out.println("RTT_ESTIMATE " + rtt);
            socket.close(); 
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}




