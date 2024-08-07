# Design Document 

# Zookeeper

Our fault-tolerant replicated datastore utilizes Zookeeper for coordination among servers,
distributed locking mechanisms, and crash recovery procedures. Each server maintains its
local database(Cassandra), and client requests, categorized as inserts key or updates key, are
seamlessly handled through a structured process. There are multiple parts which contribute to
this coordination and consensus. However some important points to note are: We have one
instance of zookeeper and every server has its own local db. All requests from the client are
stored in a queue. Previously, we elected a leader who determined the global order of requests.
Now, we used a zookeeper to come up with the global order. For each test case, one key
space is created and updates are sent randomly to servers. The challenge is receiving these
updates concurrently which we need to solve, keeping in mind that servers can crash and
recover anytime. Overall, there are 3 essential mechanisms that we are implementing:

1. Distributed Lock Mechanism
2. Watch Mechanism
3. Crash Recovery Mechanism

**Flow of execution:** We have one instance of zookeeper running on localhost:2181 and every
server has its own local db(Cassandra).

We initialize necessary data structures like PriorityQueue for message handling and
ExecutorService for asynchronous multithreaded tasks.

**Asynchronous Processing** : The use of ExecutorService and separate threads for processing
messages and events ensures efficient and non-blocking operations.

When a client sends a request to a server,requests can be either inserts or updates to the
datastore. The handleMessageFromClient method in MyDBFaultTolerantServerZK is
responsible for receiving and processing these client messages. It enqueues the messages into
a Queue (messageQueue). A separate thread, queueProcessingThread, continuously dequeues
messages from messageQueue and processes them. For each message, the server checks
whether it is an insert or an update request. If it's an insert request, the handleInsertRequest
method is called, which creates a corresponding Znode in Zookeeper to represent the new
data item. If it's an update request, the handleUpdateRequest method is called, which acquires
a distributed lock using Zookeeper to ensure mutual exclusion during the update operation. It
then updates the Znode's data in Zookeeper and triggers a data update in the Cassandra
datastore.

Previously, we elected a leader who determined the global order of requests. Now, we used a
zookeeper for the global order. Cassandra has a table “Grade”, hence in our implementation,
we also create a parent znode named “Grade.” Each server also has a table named “Grade”,
for eg. table.server1. We only have 1 Cassandra instance running in the background and each
of the servers has it own keyspace and table Grade. Whenever a server receives an insert
request, we create a child znode to the parent Grade Znode, with some unique key-name and
with value as null. On receiving an update request, the value of this key Znode is updated. All
of these requests are received as SQL statements which are then parsed and sent to the z-
node.


The challenge is receiving these updates concurrently, ie, when 2 threads simultaneously
attempt on updating the znode. Typically, a write operation is preceded by a read, and the
value to be written is appended at the end of the previous value. When 2 servers
simultaneously try to read and write to the db, lets say the read returns [1,2,3] and server 1
attempts to write 4, making the final value [1,2,3,4] and server 2 attempts to write 5, making the
final value [1,2,3,5], where we end up losing the value 4.

To prevent this, we utilize a **Distributed “Lock” Mechanism** , to ensure that multiple servers
cannot write to znode simultaneously. To do this, under the table “Grade” (parent znode) we
create another ephemeral Zookeeper node named “key_lock”, when an update request is
received, the server wants update the value of the key Znode, we check if the Grade Znode
has a child called “key_lock”. If not, the child znode “key_lock” is created, and we write events
to the Key Znode and deletes the lock Znode after it finishes writing. If the znode “key_lock”
already exists, a server waits till it gets deleted.

Another mechanism we have utilized is a “ **Watch” Mechanism:**

Whenever there is an update in a znode, it sends a notification to all the servers. For eg, if key
space key_1 exists, every Server keeps a watch on that key-Znode. Whenever there is an
update to key_1, the servers get notified, they read the value of the Key_1 Znode and converts
it to an array and compares it with its local db to check what updates have occurred, and
appends those updates to its own local db. Hence, the server sets a watch on a new Znode to
receive notifications of any changes and reacts to the Zookeeper event in case of changes,

We also use a **“Crash Recovery” Mechanism:**

During server startup we check if the parent Znode is present or not if it is present then we
initiate crash recovery phase by calling the recoverFromCrash method. This method identifies
any missed updates during a server crash by comparing the existing Znodes in Zookeeper with
the data in the Cassandra datastore.It then synchronizes the data by applying missed updates
to Cassandra, ensuring consistency between Zookeeper and Cassandra.

There are be 2 scenarios here:

1. Scenario 1 - A server crashes and other servers are active, which keep writing to znode and
    their own local db. Whenever the crashed server recovers, it misses only the updates that
    occurred after it crashed, because of the existence of a persistent db. To implement this, in
    the recovery phase, we compare the missing key values in Cassandra to the children of the
    Parent Znode and we sync the missing Keys in cassandra from the child Znodes.
2. Scenario 2 - It is also possible that a server could crash while its in the middle of executing
    a write operation. In such case, only checking for missing keys is not enough, as the server
    could have a key present but might not have the values updated. In this case,We check the
    length of the list of events in the cassandra for a key and the lists size in child znode and if
    they are different we update the events in cassandra for that key.

This flow ensures that client requests are processed in a fault-tolerant manner. The
coordination through Zookeeper, distributed locking mechanisms, and crash recovery
procedures contribute to maintaining consistency and availability in the replicated datastore
server. This comprehensive flow illustrates the handling of client requests, distributed
coordination through Zookeeper, and the synchronization of data between Zookeeper and the
Cassandra datastore in a fault-tolerant replicated environment.
Finally, the Cassandra session, cluster, zookeeper client and executorService are closed for a
graceful exit.


# Design Document 

# GigaPaxos

The MyDBReplicableAppGP class is designed using the Gigapaxos framework to facilitate
seamless execution of requests across multiple distributed replicas. The application interacts
with a Cassandra database, ensuring a robust and fault-tolerant system.

**Setup**

The constructor establishes a connection to the Cassandra database using the provided
keyspace. The methods encapsulate the core functionality, including request execution, state
checkpointing, and restoration, adhering to the Gigapaxos Replicable interface.

**Request Execution**

The execute methods handle the execution of requests, leveraging the Cassandra session to
submit queries. The boolean parameter doNotReplyToClient in Replicate class allows the
application to control whether or not to reply to the client immediately, which we have set to
false in order to send a reply back to the client. Error handling is incorporated to log issues and
return appropriate success indicators. True is returned only when a request is successfully
executed, else we log the failure and return false.

**Checkpointing**

It queries the Cassandra database to retrieve the current state and formats it into a string
representation. The method returns this formatted state string, providing a checkpoint of the
application's state. It plays a pivotal role in ensuring consistent and recoverable application
states across replicas. We generate the whole table result using SELECT * and format it row
wise using : and ; as delimiters in order to store events in the form of _eventID:eventList;_ The
final checkpoint state is stored in eventsHistory as a String in the described format.

**Restoration of lost data**

The restore method is integral for recovering the application's state from a provided state string.
It parses the input string, which is the string of eventsHistory as the application state and
ensures its validity checking if its empty, in which case there are no updates to be made as a
form of restore. It then proceeds to split data entries using the previously defined delimiters. For
each dataEntry, which is an event it expects an eventID as the first part of the event and
eventList as the second. If first part, that is ID is found empty, it assigns ID as 0. To curate the
list of events, it uses retrieveEvents method. retrieveEvents method is responsible for forming a
List<Integer> of the events, which can be used for the CQL INSERT command. This event list is
formed by formatting the second part of the dataEntry string, where we first remove the first and
last elements which are brackets and split it into a String list using comma as a delimiter. Then
we iterate through this String list of events and convert it into an Integer List. If the event is
empty, we update the integer event as 0.

Finally, the Cassandra session and cluster are closed for a graceful exit.


