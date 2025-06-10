package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync"
	"time"

	"github.com/connerohnesorge/dukdb-go/internal/purego"
)

// PoolConfig holds configuration for connection pooling
type PoolConfig struct {
	MaxOpenConns    int           // Maximum number of open connections
	MaxIdleConns    int           // Maximum number of idle connections
	ConnMaxLifetime time.Duration // Maximum connection lifetime
	ConnMaxIdleTime time.Duration // Maximum connection idle time
}

// DefaultPoolConfig returns sensible defaults for connection pooling
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		MaxOpenConns:    25,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 10 * time.Minute,
	}
}

// ConnectionPool manages a pool of DuckDB connections
type ConnectionPool struct {
	duckdb     *purego.DuckDB
	dbPath     string
	config     *PoolConfig
	
	mu         sync.RWMutex
	conns      []*pooledConn
	freeConns  []*pooledConn
	connCount  int
	
	cleanupTimer *time.Timer
	closed       bool
}

type pooledConn struct {
	conn      *Conn
	createdAt time.Time
	lastUsed  time.Time
	inUse     bool
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(duckdb *purego.DuckDB, dbPath string, config *PoolConfig) *ConnectionPool {
	if config == nil {
		config = DefaultPoolConfig()
	}
	
	pool := &ConnectionPool{
		duckdb:   duckdb,
		dbPath:   dbPath,
		config:   config,
		conns:    make([]*pooledConn, 0),
		freeConns: make([]*pooledConn, 0),
	}
	
	// Start cleanup routine
	pool.scheduleCleanup()
	
	return pool
}

// Get retrieves a connection from the pool
func (p *ConnectionPool) Get(ctx context.Context) (*Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		return nil, fmt.Errorf("connection pool is closed")
	}
	
	// Try to get a free connection
	if len(p.freeConns) > 0 {
		pc := p.freeConns[len(p.freeConns)-1]
		p.freeConns = p.freeConns[:len(p.freeConns)-1]
		pc.inUse = true
		pc.lastUsed = time.Now()
		return pc.conn, nil
	}
	
	// Check if we can create a new connection
	if p.connCount < p.config.MaxOpenConns {
		conn, err := p.createConnection()
		if err != nil {
			return nil, err
		}
		
		pc := &pooledConn{
			conn:      conn,
			createdAt: time.Now(),
			lastUsed:  time.Now(),
			inUse:     true,
		}
		
		p.conns = append(p.conns, pc)
		p.connCount++
		return conn, nil
	}
	
	// Wait for a connection to become available
	// For now, return error - could implement waiting logic
	return nil, fmt.Errorf("connection pool exhausted")
}

// Put returns a connection to the pool
func (p *ConnectionPool) Put(conn *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		conn.Close()
		return
	}
	
	// Find the pooled connection
	for _, pc := range p.conns {
		if pc.conn == conn {
			pc.inUse = false
			pc.lastUsed = time.Now()
			
			// Add to free connections if we have space
			if len(p.freeConns) < p.config.MaxIdleConns {
				p.freeConns = append(p.freeConns, pc)
			} else {
				// Too many idle connections, close this one
				p.removeConnection(pc)
			}
			return
		}
	}
}

// Close closes all connections in the pool
func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		return nil
	}
	
	p.closed = true
	
	if p.cleanupTimer != nil {
		p.cleanupTimer.Stop()
	}
	
	for _, pc := range p.conns {
		pc.conn.Close()
	}
	
	p.conns = nil
	p.freeConns = nil
	p.connCount = 0
	
	return nil
}

// Stats returns pool statistics
func (p *ConnectionPool) Stats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	inUse := 0
	for _, pc := range p.conns {
		if pc.inUse {
			inUse++
		}
	}
	
	return PoolStats{
		OpenConnections: p.connCount,
		InUse:          inUse,
		Idle:           len(p.freeConns),
		MaxOpenConns:   p.config.MaxOpenConns,
		MaxIdleConns:   p.config.MaxIdleConns,
	}
}

// PoolStats holds pool statistics
type PoolStats struct {
	OpenConnections int
	InUse          int
	Idle           int
	MaxOpenConns   int
	MaxIdleConns   int
}

func (p *ConnectionPool) createConnection() (*Conn, error) {
	// Open database
	db, err := p.duckdb.Open(p.dbPath)
	if err != nil {
		return nil, err
	}
	
	// Create connection
	conn, err := p.duckdb.Connect(db)
	if err != nil {
		p.duckdb.CloseDatabase(db)
		return nil, err
	}
	
	return &Conn{
		duckdb: p.duckdb,
		db:     db,
		conn:   conn,
	}, nil
}

func (p *ConnectionPool) removeConnection(pc *pooledConn) {
	pc.conn.Close()
	
	// Remove from conns slice
	for i, c := range p.conns {
		if c == pc {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			break
		}
	}
	
	// Remove from freeConns slice
	for i, c := range p.freeConns {
		if c == pc {
			p.freeConns = append(p.freeConns[:i], p.freeConns[i+1:]...)
			break
		}
	}
	
	p.connCount--
}

func (p *ConnectionPool) scheduleCleanup() {
	if p.config.ConnMaxIdleTime <= 0 && p.config.ConnMaxLifetime <= 0 {
		return
	}
	
	interval := p.config.ConnMaxIdleTime
	if interval <= 0 || (p.config.ConnMaxLifetime > 0 && p.config.ConnMaxLifetime < interval) {
		interval = p.config.ConnMaxLifetime
	}
	
	if interval <= 0 {
		interval = time.Minute
	} else {
		interval = interval / 2 // Clean up twice as often as the timeout
	}
	
	p.cleanupTimer = time.AfterFunc(interval, p.cleanup)
}

func (p *ConnectionPool) cleanup() {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		return
	}
	
	now := time.Now()
	
	// Remove expired connections
	for i := len(p.freeConns) - 1; i >= 0; i-- {
		pc := p.freeConns[i]
		
		expired := false
		if p.config.ConnMaxIdleTime > 0 && now.Sub(pc.lastUsed) > p.config.ConnMaxIdleTime {
			expired = true
		}
		if p.config.ConnMaxLifetime > 0 && now.Sub(pc.createdAt) > p.config.ConnMaxLifetime {
			expired = true
		}
		
		if expired {
			p.removeConnection(pc)
		}
	}
	
	// Schedule next cleanup
	p.scheduleCleanup()
}