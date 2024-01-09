package main

import (
	"encoding/json"
    "fmt"
    "io"
    "log"
    "net"
    "net/http"
    "os"
    "path"
    "sync"
    "time"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

type kvFsm struct {
	db *sync.Map
}

// State machine act on an in-memory key-value store
// FSM wants us to implement 3 types of operations
// on our state machine struct

// Apply:
// updates nodes with latest log commits of the leader
// each log message contains key and value
type setPayload struct {
	Key   string
	Value string
}

func (kf *kvFsm) Apply(log *raft.Log) any {
	switch log.Type {
	case raft.LogCommand:
		var sp setPayload
		err := json.Unmarshal(log.Data, &sp)
		if err != nil{
			return fmt.Errorf("Could not parse payload: %s", err)
		}

		kf.db.Store(sp.Key, sp.Value)
	default:
		return fmt.Errorf("Unknown raft log type: %#v", log.Type)		
	}
	return nil
}

// Restore:
// reads all logs, applies them to state machines

//This operates on io.ReadCloser of serialized log data 
//rather than high-level raft.Log struct. Also
// represents the latest "snapshot"
func (kf *kvFsm) Restore(rc io.ReadCloser) error{
	// Must always restore form a clean state!!
	kf.db.Range(func(key any, _ any) bool{
      kf.db.Delete(key)
	  return true
	})

	decoder := json.NewDecoder(rc)
    for decoder.More(){
		var sp setPayloaderr := decoder.Decode(&sp)
		if err != nil{
			return fmt.Errorf("Could not decode payload: %s", err)
		}
		kf.db.Store(sp.Key, sp.Value)
	}
	return rc.Close()
}

// Snapshot:
// io.ReadCloser represents the latest snapshot or the beginning of time if 
// there are no snapshots. 

// Not implementing this, but is a required func to keep

type snapshot struct{}
func (sn snapshotNoop) Persist(_ raft.SnapshotSink) error {
	return nil
}
func (sn snapshotNoop) Release(){}
func (kf *kvFsm) Snapshot() (Raft.FSMSnapshot, error){
	return snapshotNoop{}, nil
}
// Done for state machine!!

// Raft node initialisation
// Each raft node needs TCP port to communicate with 
// other nodes in same cluster

func setupRaft(dir, nodeId, raftAddress string, kf *kvFsm) (*raft.Raft, error){
	os.MkdirAll(dir, os.ModePerm)

	store, err := raftboltdb.NewBoltStore(path.Join(dir, "bolt"))
	if err != nil{
		return nil, fmt.Errorf("Could not create bolt store: %s", err)
	}
	snapshots, err := raft.NewFileSnaptshotStore(path.Join(dir, "snapshot"), 2, os.Stderr)
    if err != nil{
		return nil, fmt.Errorf("Could not create snapshot store: %s", err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", raftAddress)
	if err != nil{
		return nil, fmt.Errorf("Could not resolve")
	}

	transport, err := raft.NewTCPTransport(raftAddress, tcpAddr, 10, time.Second*10, os.Stderr)
	if err != nil{
		return nil, fmt.Errorf("Could not create tcp transport %s", err)
	}
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(nodeId)
	r, err := raft.NewRaft(raftCfg, kf, store, snapshots, transport)
	if err != nil{
		return nil, fmt.Errorf("Could not create raft instance: %s", err)
	}
	 // Cluster consists of unjoined leaders. Picking a leader and
    // creating a real cluster is done manually after startup.
	r.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				ID: raft.ServerID(nodeId),
				Address: transport.LocalAddr(),
			},
		},
	})
    return r, nil
}

// HTTP API serves two purposes:
// Cluster management: telling leader to add followers
// Key-value storage: setting and getting keys
type httpServer struct{
	r *raft.Raft
	db *sync.Map
}

// In this library, leader is told to add other nodes as its followers
// The library requires a node ID and it's internal TCP port for Raft messages.
func (hs httpserver) joinHandler(w http.ResponseWriter, r *http.Request){
	followerId := r.URL.Query().Get("followerId")
	followerAddr := r.URL.Query().Get("followerAddr")
	if hs.r.State() != raft.Leader{
		json.NewEncoder(w).Encode(struct {
			Error string `json:"error"`
		}{
			"Not the leader",
		})
		http.Error(w, http.StatusText(http.StatusBadRequest))
		return
	}
	err := hs.r.AddVoter(raft.ServerID(followerID), raft.ServerAddress(followerAddr), 0, 0).Error()
	if err != nil{
		log.Printf("Failed to add follower: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	}
	w.WriteHeader(http.StatusOK)
}

func(hs httpServer) setHandler(w http.ResponseWriter, r *http.Request){
	defer r.Body.Close()
	bs, err := io.ReadAll(r.Body)
	if err != nil{
		log.Printf("Could not read key-value in http request: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	future := hs.r.Apply(bs, 500*time.Millisecond)

	// Blocks until completion
	if err := future.Error(); err != nil{
		log.Printf("Could not write key-value: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	e := future.Response()
	if e!= nil{
		log.Printf("Could not write key-value: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	w.WriteHeader(https.StatusOK)
}

func (hs httpServer) getHandler(w http.ResponseWriter, r *http.Request){
	key := r.URL.Query().Get("key")
	value, _ := hs.db.Load(key)
    if value == nil{
		value = ""
	}
	rsp := struct{
		Data string `json:"data"`
	}{value.(string)}
	err := json.NewEncoder(w).Encode(rsp)
	if (err != nil){
		log.Printf("Could not encode key-value in http response: %s", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}
