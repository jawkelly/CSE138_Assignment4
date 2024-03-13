from flask import Flask, jsonify, request
import requests
import os
import logging

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
def initialize_kvs():
    if viewenv:
        if len(replicas) == 1: # if there is only 1 replica in the view
            return
        if replicas[0] == socket_address:
            existingreplica = replicas[1]
        else:
            existingreplica = replicas[0] #choose first replica to take storage from
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

def broadcast_to_replicas(method, key, data):
    # format data for sending to other replicas
    data["causal-metadata"]= {"senders-address": socket_address, "message-clock": vc.clock}
    for replica in replicas:
        if replica != socket_address:
            try:
                replica_url = f'http://{replica}/replica_kvs/{key}'
                response = requests.request(method, replica_url, json=data)
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

    if request.method == 'PUT':
        data = request.get_json()   #returns dictionary
        if data and ('value' in data) and ('causal-metadata' in data):
            value = data['value']   #pulls value from json body
            metadata = data['causal-metadata'] #pulls metadata

            if handle_client_metadata(metadata): 
                if key in storage:    #key already exists
                    storage[key] = value
                    vc.increment(socket_address)
                    broadcast_to_replicas(request.method, key, data)
                    return jsonify({"result": "replaced", "causal-metadata": {"message-clock": vc.clock}}), 200 #INCLUDE NEW METADATA
                else:   #key does not exist
                    storage[key] = value
                    vc.increment(socket_address)
                    broadcast_to_replicas(request.method, key, data)
                    return jsonify({"result": "created", "causal-metadata": {"message-clock": vc.clock}}), 201 #INCLUDE NEW METADATA
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
                    return jsonify({"result": "found", "value": value, "causal-metadata": {"message-clock": vc.clock}}), 200 #INCLUDE NEW METADATA
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
                    broadcast_to_replicas(request.method, key, data)
                    return jsonify({"result": "deleted", "causal-metadata": {"message-clock": vc.clock}}), 200 #INCLUDE NEW METADATA
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




if __name__ == '__main__':
    storage = {}
    replicas = [] #List of all replica addresses
    #Load environment variables and VectorClock
    try:
        socket_address = os.environ.get('SOCKET_ADDRESS')
        viewenv = os.environ.get('VIEW').split(',')
        replicas.extend(viewenv)
        vc = VectorClock(replicas)
    except:
        print("No environment variables detected.")
    broadcast_put_view(socket_address)
    initialize_kvs()
    host, port = os.getenv("SOCKET_ADDRESS").split(':')
    app.run(host=host, port=port)