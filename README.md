# Project 1: Distributed File System With Probabilistic Routing #

See the project spec here: https://www.cs.usfca.edu/~mmalensek/cs677/assignments/project-1.html

## Controller ##
The Controller is responsible for managing resources in the system, somewhat like an HDFS NameNode. When a new storage node joins your DFS, the first thing it does is contact the Controller. At a minimum, the Controller contains the following data structures:

* A list of active storage nodes
* A routing table (set of bloom filters for probabilistic lookups)

## Storage Node ##

Storage nodes are responsible for storing and retrieving file chunks. When a chunk is stored, it will be checksummed so on-disk corruption can be detected. When a corrupted file is retrieved, it should be repaired by requesting a replica before fulfilling the client request.
functions:
* Store chunk [File name, Chunk Number, Chunk Data]
* Get number of chunks [File name]
* Get chunk location [File name, Chunk Number]
* Retrieve chunk [File name, Chunk Number]
* List chunks and file names [No input]

## Client ##

The client’s main functions include:
* Breaking files into chunks, asking the controller where to store them, and then sending them to the appropriate storage node(s).
  * Note: Once the first chunk has been transferred to its destination storage node, that node will pass    replicas along in a pipeline fashion. The client should not send each chunk 3 times.
    * If a file already exists, replace it with the new file. If the new file is smaller than the old, you are not required to remove old chunks (but file retrieval should provide the correct data).
* Retrieving files in parallel. Each chunk in the file being retrieved will be requested and transferred on a separate thread. Once the chunks are retrieved, the file is reconstructed on the client machine.
