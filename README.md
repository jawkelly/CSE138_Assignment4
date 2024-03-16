# CSE138_Assignment4
# OVERVIEW  
This program is a sharded, replicated, fault tolerant, and causally consistent key value store.  
Replicas communicate key-value updates within their shards, keeping everything up to date.  
Each shard is responsible for handling a portion of key-value pairs, which are all agreed upon  
by a method of hash of key. This way, nodes can tell which shard a key value pair should go to.  
The distributed system is fault tolerant, as each shard has multiple replicas. This means that when one of the replicas goes down almost no data is lost. This program supports *view* operations; PUT, GET, and   
DELETE which are responsible for managing the replicas. The same operations are available  
for the Key Value Store. The program also supports *shard* operations, which determines how  
many 'shards' the database is split up into.
  
## KEY-TO-SHARD MAPPING MECHANISM
The key to shard mapping mechanim uses hashing to determine which shard the key belongs to. 
Hashing provides a deterministic way to find the correct shard for the key. The hash value 
obtained from hashing the key using an MD5 hash generator. It is then used to determine the shard to which the key belongs. This is typically done by taking the modulus (%) of the hash value with the total number of shards (shard_count). The result is an integer representing the shard ID. Now, instead of braodcasting key value requests accross all replicas, they are kept within the shard. By using a 
hash function the kv pairs will have an even distribution across the shards. 

  
## RESHARDING MECHANISM
The resharding mechanism was maybe the most difficult part of this assignment. The first  
step is to check the number of shards that the client is requesting for the reshard, and  
how many nodes are in the current view. If two nodes would not be allocated for every shard,  
a 400 error is returned. If else, the reciever requests the entire data store from all other  
shards, before reassigning all nodes to their new shards. Shard reassignment is done by sorting the existing view, then iterating through and assigning each a shard which is one higher than the previous,  
until it reaches the last shard at which point it returns to zero. After the new shard dictionary is created, it is sent to all other replicas in the view. Afterwards, the program uses similar arithmetic as described above to assign keys(based on their hash value) to their new shards, which are then redistributed based on our new shard dictionary.
  
### TEAM CONTRIBUTIONS    
Hunter Shepston - Implemented Get shard functions and initial Reshard function.

Ali Ali - Implemented handling of key value operations so that they work properly 
on shards. Worked on shard initialization and helped with /shard requests.

Jack - Implemented causal-consistency and down detection that was missing from the last 
assignment. Created the initial shard/add-member function.

### AKNOWLEDGEMENTS  
We did not consult anyone outside of this group
  
### CITATIONS  
Python requests manual - https://pypi.org/project/requests/  
Used to figure out message sending, especially for optional arguments on requests.request().
This was used throughout the program, but was especially helpful in finding out how to handle
metadata.
  
Python time manual - https://docs.python.org/3/library/time.html  
Used to look up the 'sleep' function, useful when a message returned a 503 but had to be 
delivered eventually. The function would resend the request after short sleeps until the
causal dependencies were satisfied for a replica.

"What is Database Sharding" - https://www.youtube.com/watch?v=hdxdhCpgYo8&ab_channel=BeABetterDev  
Used in conjunction with course material to gain an understanding of the concept of sharding.
