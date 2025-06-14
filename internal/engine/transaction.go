package engine

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// TransactionState represents the state of a transaction
type TxnState int

const (
	TxnStateActive TxnState = iota
	TxnStateCommitted
	TxnStateAborted
)

var transactionIDCounter atomic.Uint64

// generateTransactionID generates a unique transaction ID
func generateTransactionID() uint64 {
	return transactionIDCounter.Add(1)
}

// Transaction represents a database transaction
type Transaction struct {
	id         uint64
	connection *Connection
	state      TxnState
	startTime  time.Time
	mu         sync.Mutex
	
	// Transaction isolation and MVCC support
	readTimestamp  uint64
	writeTimestamp uint64
	
	// Write set for tracking modifications
	writeSet map[string]*WriteSetEntry
}

// WriteSetEntry tracks modifications to a table
type WriteSetEntry struct {
	tableName string
	inserts   []RowID
	updates   map[RowID]interface{}
	deletes   []RowID
	
	// Store actual data for inserts
	pendingInserts [][]interface{}
	// Store update data: map of column index to new value for each row
	pendingUpdates map[int]map[int]interface{} // map[rowIndex]map[colIndex]value
}

// RowID uniquely identifies a row
type RowID struct {
	ChunkID uint32
	RowIdx  uint32
}

// NewTransaction creates a new transaction
func NewTransaction(conn *Connection) *Transaction {
	return &Transaction{
		id:            generateTransactionID(),
		connection:    conn,
		state:         TxnStateActive,
		startTime:     time.Now(),
		writeSet:      make(map[string]*WriteSetEntry),
		readTimestamp: getCurrentTimestamp(),
	}
}

// Commit commits the transaction
func (t *Transaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	if t.state != TxnStateActive {
		return fmt.Errorf("transaction is not active")
	}
	
	// Get write timestamp
	t.writeTimestamp = getNextTimestamp()
	
	// Apply write set
	for tableName, entry := range t.writeSet {
		// Get the table from catalog
		catalog := t.connection.database.catalog
		schema, err := catalog.GetSchema("main")
		if err != nil {
			return fmt.Errorf("failed to get schema: %w", err)
		}
		
		table, err := schema.GetTable(tableName)
		if err != nil {
			return fmt.Errorf("failed to get table %s: %w", tableName, err)
		}
		
		// Apply pending inserts
		if len(entry.pendingInserts) > 0 {
			if err := table.Insert(entry.pendingInserts); err != nil {
				return fmt.Errorf("failed to apply inserts to table %s: %w", tableName, err)
			}
		}
		
		// Apply pending updates
		if entry.pendingUpdates != nil && len(entry.pendingUpdates) > 0 {
			if err := table.UpdateRows(entry.pendingUpdates); err != nil {
				return fmt.Errorf("failed to apply updates to table %s: %w", tableName, err)
			}
		}
		
		// Apply deletes
		// TODO: Implement delete logic
	}
	
	t.state = TxnStateCommitted
	return nil
}

// Rollback rolls back the transaction
func (t *Transaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	if t.state != TxnStateActive {
		return fmt.Errorf("transaction is not active")
	}
	
	// Discard write set
	t.writeSet = nil
	t.state = TxnStateAborted
	
	return nil
}

// AddInsert records an insert operation
func (t *Transaction) AddInsert(tableName string, rowID RowID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	entry := t.getOrCreateWriteSetEntry(tableName)
	entry.inserts = append(entry.inserts, rowID)
}

// AddUpdate records an update operation
func (t *Transaction) AddUpdate(tableName string, rowID RowID, data interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	entry := t.getOrCreateWriteSetEntry(tableName)
	if entry.updates == nil {
		entry.updates = make(map[RowID]interface{})
	}
	entry.updates[rowID] = data
}

// AddDelete records a delete operation
func (t *Transaction) AddDelete(tableName string, rowID RowID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	entry := t.getOrCreateWriteSetEntry(tableName)
	entry.deletes = append(entry.deletes, rowID)
}

// getOrCreateWriteSetEntry gets or creates a write set entry for a table
func (t *Transaction) getOrCreateWriteSetEntry(tableName string) *WriteSetEntry {
	if entry, exists := t.writeSet[tableName]; exists {
		return entry
	}
	
	entry := &WriteSetEntry{
		tableName: tableName,
		inserts:   make([]RowID, 0),
		updates:   make(map[RowID]interface{}),
		deletes:   make([]RowID, 0),
	}
	t.writeSet[tableName] = entry
	return entry
}

// Timestamp management for MVCC
var (
	timestampCounter atomic.Uint64
)

// getCurrentTimestamp returns the current timestamp
func getCurrentTimestamp() uint64 {
	return timestampCounter.Load()
}

// getNextTimestamp returns the next timestamp
func getNextTimestamp() uint64 {
	return timestampCounter.Add(1)
}