package timesync;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;

public class TSServer {
    public static void main(String[] args) {
        int port = 15750; 

        try (DatagramSocket socket = new DatagramSocket(port)) {
            System.out.println("Server is running and waiting for requests...");

            while (true) {
                byte[] receiveData = new byte[48];
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                socket.receive(receivePacket);

                InetAddress clientAddress = receivePacket.getAddress();
                int clientPort = receivePacket.getPort();

                // Implementing Multithreading
                Thread clientHandlerThread = new Thread(new ClientHandler(socket, receivePacket));
                clientHandlerThread.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class ClientHandler implements Runnable {
        private DatagramSocket socket;
        private DatagramPacket receivePacket;

        public ClientHandler(DatagramSocket socket, DatagramPacket receivePacket) {
            this.socket = socket;
            this.receivePacket = receivePacket;
        }

        @Override
        public void run() {
            try {
                byte[] sendData = getNTPResponse(receivePacket.getData());
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length,
                        receivePacket.getAddress(), receivePacket.getPort());
                socket.send(sendPacket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static byte[] getNTPResponse(byte[] request) {
        ByteBuffer requestBuffer = ByteBuffer.wrap(request);
        ByteBuffer responseBuffer = ByteBuffer.allocate(48);

        //Client Transmit Time
        //As timestamp is 8 bytes long, retrieving at position 40
        long clientTransmitTime = requestBuffer.getLong(40);
        responseBuffer.putLong(clientTransmitTime);
        //Server Receive Time
        long serverReceiveTime = System.currentTimeMillis();
        responseBuffer.putLong(serverReceiveTime);
        //Server Transmit Time
        long serverTransmitTime = System.currentTimeMillis();
        responseBuffer.putLong(serverTransmitTime);
        return responseBuffer.array();
    }
}
