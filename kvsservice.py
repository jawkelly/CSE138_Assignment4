from flask import Flask, jsonify, request
import requests
import os
import logging
import hashlib

app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG)

# print(replicas)
ERRMSG = "Method Not Allowed"

### VECTOR CLOCK ###
class VectorClock: # view is Vector Clock
    # initialize clock as dictionary
    def __init__(self, replicas):
        self.clock = {}
        for replica in replicas:
            self.add_replica(replica)

    # used to increment the host clock at the senders position
    def increment(self, sender_address):
        if sender_address in self.clock.keys():
            self.clock[sender_address] += 1
        else:
            raise KeyError(f"Key: {self.socket} not in vc. // increment()")
    
    def add_replica(self, replica_address):
        if replica_address not in self.clock.keys():
            # raise KeyError(f"Key: {replica_address} already in vc. // add_replica()")
            self.clock[replica_address] = 0

    def delete_replica(self, replica_address):
        if replica_address in self.clock.keys():
            del self.clock[replica_address]

    # Compare two vector clocks.
    # Return boolean True if replica_vc happens before or is concurrent to other_clock, False otherwise
    def is_causal(self, message_clock, sender_address=None):
        if not isinstance(message_clock, dict):
            raise TypeError("Other clock must be a dictionary. // is_causal()")
        
        if sender_address != None: # None indicates a client call and !none is from replica
            if message_clock[sender_address] != self.clock[sender_address] + 1:
                return False

        for replica_address in self.clock:
            if replica_address != sender_address: # exclude the sender address
                if replica_address in message_clock:
                    if message_clock[replica_address] > self.clock[replica_address]:
                        return False
                # else: # CLIENT COULD HAVE A MISSING VC BUT THE REPLICAS WILL NOT
                #     raise KeyError(f"Key: {replica_address} from local clock not in message vc. // update_from_message()")     
        return True
        
def forwarding_request(method, key, fwdaddress):
        url = f"http://{fwdaddress}/kvs/{key}"  #format url that we will forward to
        try:
            #forward request to url, store response
            response = requests.request(method, url, headers=request.headers, data=request.get_data())
            return response.json(), response.status_code        #return response
        except: #case of failure
            return jsonify({"error": "Cannot forward request"}), 503

## Used to broadcast a new replica's address to all replicas in its initial view
def broadcast_put_view(sentaddress):
    data = {'socket-address': sentaddress}
    for replica in replicas:
        if replica != sentaddress:
            try:
                # response = requests.put(f'http://{replica}/view', json=data, headers={'Content-Type': 'application/json'})
                response = requests.put(f'http://{replica}/view', json=data)
                print(f'broadcast_put_view: {replica} response:', response.json())
            except requests.exceptions.RequestException as e:
                print(f'broadcast_put_view error: exception raised: {e}')



## Used to copy existing storage from a kvs when a new replica is made
## Now should also copy shards and vector clock (done in /getall)
def initialize_kvs(id): #now wait until put add-member before using this
    global shards
    if viewenv:
        if len(replicas) == 1: # if there is only 1 replica in the view
            return
        if replicas[0] == socket_address:
            existingreplica = replicas[1]
        else:
            existingreplica = replicas[0] #choose first replica to take storage from

        #First retrieve shards dictionary so we can pull kvs from correct shard
        try:
            url = f'http://{existingreplica}/getshards'
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                shards = {int(k): v for k, v in data["shards"].items()} #this converts all the keys to ints bc json will return them as strings
        except requests.exceptions.RequestException as e:
            print(f"initialize_kvs failed:  exception raised {e}")

        # app.logger.debug(shards)
        #change existing replica to be a replica in shards so we can pull data from the correct shard
        existingreplica = shards[id][0]
        try:
            url = f'http://{existingreplica}/getall'
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                vc.clock = data["message-clock"] # copy the clock from the previously existing replica
                for key, value in data["storage"].items():
                    storage[key] = value
            else:
                print(f"initialize_kvs failed: status code from getall - {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"initialize_kvs failed:  exception raised {e}")

def broadcast_delete_view(sent_address):
    data = {'socket-address': sent_address}
    replicas.remove(sent_address) # remove from view here
    vc.delete_replica(sent_address)
    for replica in replicas:
        if replica != sent_address:
            try:
                # response = requests.put(f'http://{replica}/view', json=data, headers={'Content-Type': 'application/json'})
                response = requests.delete(f'http://{replica}/view', json=data)
                print(f'broadcast_delete_view: {replica} response:', response.json())
            except requests.exceptions.RequestException as e:
                print(f'broadcast_delete_view error: exception raised: {e}')

def broadcast_to_replicas(method, key, data, shard_id):
    # format data for sending to other replicas
    data["causal-metadata"]= {"senders-address": socket_address, "message-clock": vc.clock}

    #Broadcast request to replicas in shard_id
    for replica in shards[shard_id]:
        if replica != socket_address:
            try:
                replica_url = f'http://{replica}/replica_kvs/{key}'
                response = requests.request(method, replica_url, json=data)
            except requests.exceptions.RequestException as e:
                broadcast_delete_view(replica)
                app.logger.debug(f'broadcast_to_replicas error: exception raised: {e}')

    #Broadcast update to metadata for all non-shard_id replicas
    for other_id, other_id_replicas in shards.items():
        if other_id != shard_id:    #Request goes to all shards that aren't shard_id
            for replica in other_id_replicas:
                try:
                    url = f'http://{replica}/update_metadata'
                    requests.put(url, json={"causal-metadata": data["causal-metadata"]})
                except requests.exceptions.RequestException as e:
                    broadcast_delete_view(replica)
                    app.logger.debug(f'broadcast_to_replicas error: exception raised: {e}')

        
def handle_client_metadata(metadata):
    if metadata == None: # clients first request so the causal-metadata is null
        return True
    else:
        message_clock = metadata["message-clock"]

    app.logger.debug(f'IN CLIENT METADATA: \nLOCAL CLOCK: {vc.clock}\n MESSAGE CLOCK: {message_clock}')
    if vc.is_causal(message_clock):
        return True
    else:
        return False
    #for checking causal consistency of passed in metadata

def handle_replica_metadata(metadata):
    if metadata == None:
        return False
    else:
        message_clock = metadata["message-clock"]
        senders_address = metadata["senders-address"]

    app.logger.debug(f'IN REPLICA METADATA: \nSENDERS_ADDRESS: {senders_address}\nLOCAL CLOCK: {vc.clock}\n MESSAGE CLOCK: {message_clock}')
    if vc.is_causal(message_clock, senders_address):
        vc.increment(senders_address)
        return True
    else:
        return False
    #for checking causal consistency of passed in metadata

#Shard assignment by hash of key, each key is assigned a unique shard based on its keyhash
def hash_of_key(key):
    app.logger.debug(f'shard count is {shard_count}')
    keyhash = int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)
    return keyhash % shard_count

@app.route('/getshards', methods=['GET'])
def getshards():
    return jsonify({"shards": shards}), 200

@app.route('/getall', methods=['GET'])
def getall():
    return jsonify({"storage": storage, "message-clock": vc.clock}), 200

@app.route('/replica_kvs/<key>', methods=['GET', 'PUT', 'DELETE'])
def replica_kvs(key):

    if len(key) > 50:
        return jsonify({"error": "Key is too long"}), 400
    
    if request.method == 'PUT':
        data = request.get_json()   #returns dictionary
        if data and ('value' in data) and ('causal-metadata' in data):
            value = data['value']   #pulls value from json body
            metadata = data['causal-metadata'] #pulls metadata

            if handle_replica_metadata(metadata): 
                if key in storage:    #key already exists
                    storage[key] = value
                    return jsonify({"result": "replaced", "causal-metadata": metadata}), 200 #INCLUDE NEW METADATA
                else:   #key does not exist
                    storage[key] = value
                    print("here in replica_kvs")
                    return jsonify({"result": "created", "causal-metadata": metadata}), 201 #INCLUDE NEW METADATA
            else:
                return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503
            
        else:   #passed in data is invalid
            return jsonify({"error": "PUT request does not specify a value or metadata"}), 400
        
    if request.method == 'DELETE':
        data = request.get_json()   #returns dictionary
        if data and 'causal-metadata' in data:
            metadata = data['causal-metadata']
            if handle_replica_metadata(metadata):
                if key in storage:
                    storage.pop(key)
                    return jsonify({"result": "deleted", "causal-metadata": metadata}), 200 #INCLUDE NEW METADATA
                else:
                    return jsonify({"error": "Key does not exist"}), 404
            else:
                return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503
        else:
            return jsonify({"error": "DELETE request does not specify metadata"}), 400
    

@app.route('/kvs/<key>', methods=['GET', 'PUT', 'DELETE'])
def kvs(key):

    if len(key) > 50:
        return jsonify({"error": "Key is too long"}), 400
    
    print("here in kvs")
    shard_id = hash_of_key(key)
    #Key does not belong to this replica's shard (based on its hash)
    if socket_address not in shards[shard_id]:
        correct_shard_replica = shards[shard_id][0]
        forward_url = f"http://{correct_shard_replica}/kvs/{key}"
        response = requests.request(request.method, forward_url, json=request.get_json(), headers={"Content-Type": "application/json"})
        return jsonify(response.json()), response.status_code        #SHOULD RETURN SHARD ID ASWELL !!!!!
    
    if request.method == 'PUT':
        data = request.get_json()   #returns dictionary
        if data and ('value' in data) and ('causal-metadata' in data):
            value = data['value']   #pulls value from json body
            metadata = data['causal-metadata'] #pulls metadata

            if handle_client_metadata(metadata): 
                if key in storage:    #key already exists
                    storage[key] = value
                    vc.increment(socket_address)
                    broadcast_to_replicas(request.method, key, data, shard_id)
                    return jsonify({"result": "replaced", "causal-metadata": {"message-clock": vc.clock}, "shard-id": shard_id}), 200 #INCLUDE NEW METADATA
                else:   #key does not exist
                    storage[key] = value
                    vc.increment(socket_address)
                    broadcast_to_replicas(request.method, key, data, shard_id)
                    return jsonify({"result": "created", "causal-metadata": {"message-clock": vc.clock}, "shard-id": shard_id}), 201 #INCLUDE NEW METADATA
            else:
                return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503
            
        else:   #passed in data is invalid
            return jsonify({"error": "PUT request does not specify a value or metadata"}), 400

    if request.method == 'GET':
        data = request.get_json()   #returns dictionary
        if data and 'causal-metadata' in data:
            metadata = data['causal-metadata']
            if handle_client_metadata(metadata):
                if key in storage:
                    value = storage[key]
                    # Could be problems below
                    return jsonify({"result": "found", "value": value, "causal-metadata": {"message-clock": vc.clock}, "shard-id": shard_id}), 200 #INCLUDE NEW METADATA
                else:
                    return jsonify({"error": "Key does not exist"}), 404
            else:
                return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503
        else:
            return jsonify({"error": "GET request does not specify metadata"}), 400
        
    if request.method == 'DELETE':
        data = request.get_json()   #returns dictionary
        if data and 'causal-metadata' in data:
            metadata = data['causal-metadata']
            if handle_client_metadata(metadata):
                if key in storage:
                    storage.pop(key)
                    vc.increment(socket_address)
                    broadcast_to_replicas(request.method, key, data, shard_id)
                    return jsonify({"result": "deleted", "causal-metadata": {"message-clock": vc.clock}, "shard-id": shard_id}), 200 #INCLUDE NEW METADATA
                else:
                    return jsonify({"error": "Key does not exist"}), 404
            else:
                return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503
        else:
            return jsonify({"error": "DELETE request does not specify metadata"}), 400

   
@app.route('/view', methods=['GET', 'PUT', 'DELETE'])
def view():
    if request.method == 'PUT':
        data = request.get_json()
        if data and 'socket-address' in data:
            socket_address = data['socket-address'] #pulls socket address from json body
            if socket_address in replicas:
                return jsonify({"result": "already present"}), 200
            else: 
                replicas.append(socket_address)
                vc.add_replica(socket_address)
                return jsonify({"result": "added"}), 201
        else:
            return jsonify({"error": "PUT request does not specify a socket-address"})
        
    if request.method == 'GET':
        return jsonify({"view": replicas}), 200
    
    if request.method == 'DELETE':
        data = request.get_json()
        if data and 'socket-address' in data:
            socket_address = data['socket-address'] #pulls socket address from json body
            if socket_address in replicas:
                replicas.remove(socket_address)
                vc.delete_replica(socket_address)
                return jsonify({"result": "deleted"}), 200
            else:
                return jsonify({"error": "View has no such replica"}), 404
        else:
            return jsonify({"error": "DELETE request does not specify a socket-address"})

@app.route('/update_metadata', methods=['PUT'])
def update_metadata():
    data = request.get_json() #data = causal metadata
    if 'causal-metadata' in data:
        metadata = data['causal-metadata']
        if handle_replica_metadata(metadata):
            return jsonify({"result": "metadata updated"}), 200
        else:
            return jsonify({"result": "metadata failed to update"}), 503 #RETRY UNTIL SUCCESS HERE
    else:
        return jsonify({"error": "Request does not contain 'causal-metadata'"}), 400

     
@app.route('/shard/ids', methods=['GET'])
def get_shard_ids():
    return jsonify({"shard-ids": list(shards.keys())}), 200

@app.route('/shard/node-shard-id', methods=['GET'])
def get_node_shard_ids():
    for shard_id, nodelist in shards.items():
        if socket_address in nodelist:
            return jsonify({"node-shard-id": shard_id}), 200
    return jsonify({"node-shard-id": "Not found(should not occur)"}), 404

@app.route('/shard/members/<ID>', methods=['GET'])
def get_members(ID):
    if int(ID) in shards:
        return jsonify({"shard-members": shards[int(ID)]}), 200
    else:
        return jsonify({"shard-members": "Not found"}), 400
    
@app.route('/shard/key-count/<ID>', methods=['GET'])
def get_keycount(ID):
    shard_id = int(ID)
    if shard_id in shards:
        counter = 0
        if socket_address in shards[shard_id]:
            return jsonify({"shard-key-count": len(storage)}), 200
        else:
            correct_shard_replica = shards[shard_id][0]
            url = f"http://{correct_shard_replica}/shard/key-count/{shard_id}"
            try:
                response = requests.get(url)
                return jsonify(response.json()), response.status_code
            except requests.exceptions.RequestException as e:
                app.logger.debug(f'get_keycount error: exception raised: {e}')
    else:
        return jsonify({"error": "Shard does not exist"}), 404

#For client request to add-member
@app.route('/shard/add-member/<ID>', methods=['PUT'])
def add_member(ID):
    id = int(ID)
    if request.method == 'PUT':
        data = request.get_json()
        node_id = data.get('socket-address')
        if node_id == socket_address:
                initialize_kvs(id)
        if id in list(shards.keys()) and node_id in replicas:
            # add {"node_id": shard_id} to shard_view
            shards[id].append(node_id)
            for replica in replicas:
                if replica != socket_address:
                    url = f"http://{replica}/shard/broadcast-add-member/{id}"
                    response = requests.put(url, json=data)
            return jsonify({"result": "node added to shard"}), 200
        else:
            return jsonify({"error": "shard_id not found in shard_list or node_id not found in shard_view"}), 404
    else:
        return 400 # unkown error (temporary)

#Same as add-member, but does not broadcast
#Returns 404 for new replica (when node_id == socket_address) bc shards{} is empty, so there are no shard keys.
    #Need to make seperate case where retrieve kvs, shards, and metadata is handled before
@app.route('/shard/broadcast-add-member/<ID>', methods=['PUT'])
def broadcast_add_member(ID):
    id = int(ID)
    if request.method == 'PUT':
        data = request.get_json()
        node_id = data.get('socket-address')
        if node_id == socket_address:
                initialize_kvs(id)
        if id in list(shards.keys()) and node_id in replicas:
            # add {"node_id": shard_id} to shard_view
            shards[id].append(node_id)
            # if(node_id == socket_address):
                #retrieve kvs, shards, and vector clock
            return jsonify({"result": "node added to shard"}), 200
        else:
            return jsonify({"error": "shard_id not found in shard_list or node_id not found in shard_view"}), 404
    else:
        return 400 # unkown error (temporary)

@app.route('/shard/reshard')
def reshard():

    num_shards = float(request.get_json()['shard_count'])
    lengthfloat = float(len(replicas))
    if (lengthfloat / num_shards) < 2:
        return jsonify({"error": "Not enough nodes to provide fault tolerance with requested shard count"}), 400
    else:
        global shard_count 
        shard_count = num_shards
        localshard = -1
        shards = {}
        # for i in range(num_shards):    #reassigns list with all shards numbered
        #     shard_list[i] = i
        replicas.sort()
        for x in range (len(replicas)):
            shard = x % num_shards
            shards[shard].append(replicas[x]) #assigns each IP a shard in the global view
            if replicas[x] == socket_address:
                localshard = shard    #set local shard id
        
        for replica in replicas:
            url = f"http://{replica}/shard/reshard/broadcasted" 
            response = requests.put(url, json=request.get_json())

        for key in storage:
            keyshard = hash_of_key(key)
            if localshard != keyshard:
                broadcast_to_replicas('PUT', key, storage[key], keyshard)
                del storage[key]  #delete item from local storage

@app.route('/shard/reshard/broadcasted')
def reshard_broadcasted():

    num_shards = float(request.get_json()['shard_count'])
    lengthfloat = float(len(replicas))
    if (lengthfloat / num_shards) < 2:
        return jsonify({"error": "Not enough nodes to provide fault tolerance with requested shard count"}), 400
    else:
        global shard_count 
        shard_count = num_shards
        localshard = -1
        shards = {}
        # for i in range(num_shards):    #reassigns list with all shards numbered
        #     shard_list[i] = i
        replicas.sort()
        for x in range (len(replicas)):
            shard = x % num_shards
            shards[shard].append(replicas[x]) #assigns each IP a shard in the global view
            if replicas[x] == socket_address:
                localshard = shard    #set local shard id

        for key in storage:
            keyshard = hash_of_key(key)
            if localshard != keyshard:
                broadcast_to_replicas('PUT', key, storage[key], keyshard)
                del storage[key]  #delete item from local storage



if __name__ == '__main__':
    storage = {}
    replicas = [] #List of all replica addresses
    shards = {}
    shard_count = 0
    try:
        socket_address = os.environ.get('SOCKET_ADDRESS')
        viewenv = os.environ.get('VIEW').split(',')
        replicas.extend(viewenv)
        vc = VectorClock(replicas)
    except:
        print("No environment variables detected.")
    
    #On initial startup, all nodes are given shardcount, but afterwards, nodes must be assigned
    try:
        shard_count = int(os.environ.get('SHARD_COUNT'))
        shards = {i: [] for i in range(shard_count)} #initializes shards list, maps shard_count amount of shards to empty lists
        shard_id = 0
        #Iterate through our shard dictionary adding each node to one shard at a time for even distribution
        for replica in replicas:
            if shard_id == shard_count:
                shard_id = 0
            shards[shard_id].append(replica)
            shard_id += 1
        print(shards)
    except:
        shards = {}
        print("Shard_count not specified, wait for add-member request")
    
        
    #Load environment variables and VectorClock
    broadcast_put_view(socket_address)
    host, port = os.getenv("SOCKET_ADDRESS").split(':')
    app.run(host=host, port=port)
