# Simple Distributed System : Time Synchronization Server and Client

# Design Summary:

## Communication Protocol : UDP

## Time Synchronization Protocol : NTP

## Language used : JAVA

The Time Synchronization (TS) system is designed to simulate time synchronization between a client and
a server in a simple distributed system. For time synchronization, quick exchange of data is crucial,
therefore the communication protocol chosen was UDP as it offers low latency and overhead.

To make time synchronization accurate and robust, the synchronization protocol we chose to implement
is NTP (Network Time Protocol). NTP is commonly used in critical applications that demand precision
and is a well-established protocol making it a dependable choice.

The messages exchanged between the client and server are UDP packets, where the **Client Request
Message** contains Mode, Version, Leap Indicator (for indicating a flag for NTP), and Client Transmit
Timestamp. Whereas, **Server Response Message** includes Client Transmit Timestamp, Server Receive
Timestamp and Server Transmit Timestamp. This data is transmitted as bytes where each timestamp is 8
bytes long.

We have ensured that our design allows the server to talk to multiple client instances simultaneously
communicating with it by using multithreading. It handles multiple client requests simultaneously,
increasing responsiveness.

The design synchronizes time between the client and server and this synchronized time is stored in a
local variable “ **synchronizedTime** ” which is calculated as **serverTransmitTime + offset.**

TSServer.java functionality : The server continuously listens on a specific port for incoming requests. On
receiving a request, it initiates a new thread (ClientHandler) to process the client's request. This
approach guarantees that the server can handle multiple clients simultaneously in a non-blocking
manner. DatagramSocket establishes a socket for listening to incoming UDP packets. ClientHandler
allows concurrent handling of multiple clients by acting as an individual thread responsible for handling
each client's request. Within getNTPResponse, the client's transmit time is extracted, the server's time is
recorded, and this data is then encapsulated within a response packet.

TSClient.java functionality : The client code is used to send time synchronization requests. It can allows
optional command-line arguments for customizing the server IP address and port.ByteBuffer ensures
correct parsing during packing and unpacking the data.The server receive time and server transmit time
are recorded using System.currentTimeMillis(). These values are returned to the client and used for
calculating the RTT, Offset, Remote time, Local time and Synchronized Time.


