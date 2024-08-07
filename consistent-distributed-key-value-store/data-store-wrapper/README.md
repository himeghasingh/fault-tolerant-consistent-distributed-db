# Design Document 

**Client Design:** MyDBClient.java is the client component and is responsible for sending
requests to the server and receiving/handling the response from the server
asynchronously. The client uses a unique request identifier in each request, to match a
received response with the corresponding previously sent request.
This is done in callbackSend() by using:

- **long requestId = enqueueRequest(request);** -> to generate a unique request
    identifier which increments by 1, each time the method is invoked.
- **callbackMap.put(requestId, callback);** -> to maintain a map between requestIDs
    and their corresponding callbacks. We use a synchronized map, as that provides
    thread safety in a multithreaded environment, ensuring that only one thread can
    access it at a time.
- The request is then appended with the requestID and this is sent to the server using
    **super.send(isa, requestWithId);** which utilizes the send() function in the parent
    Client class, which sends requests to the server in a non-blocking way, that is, after
    sending the request, the client doesn’t stay blocked waiting for a response from the
    server, but can rather engage in performing other tasks. Once a response is
    available it is handled in the method **handleResponse().**
The response is handled in MyDBClient.java in the following way:
- The requestID is first extracted from the response, and it is used as a key to fetch
    its respective callback.
- If a callback is available, **callback.handleResponse(bytes, header)** in the parent
    Client class is invoked to handle this response.
- Once the callback has been handled, the requestID is removed from the map in
    callbackMap.remove(requestId); as the request has been catered to.

**Server Design:** MyDBSingleServer.java is the server component and is responsible for
receiving client requests, executing those requests and sending back the response to
the client. This is done in the method **handleMessageFromClient()**. It is done in the
following way:

- Here the requests involves querying and updating a Cassandra database. Hence,
    the server first initiates a connection with the Cassandra database.
- The method parses the client request to check if its an asynchronous request, by
    checking if it contains a colon, and if it does, it extracts the requestID and marks it
    as an asynchronous request. It then executes the request on the Cassandra
    database using **ResultSet resultSet = session.execute(requestMessage);** and
    stores the result in resultSet.
- Next, we process the resultSet using StringBuilder to convert it to a usable string
    and create our server response. If the request was asynchronous, we append the
    request ID to the server response.
- Now, the response is ready to be sent back to the client and that is done using
    **this.clientMessenger.send().**
At the end, the session is closed as a part of the cleanup.


