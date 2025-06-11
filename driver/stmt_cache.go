package driver

import (
	"container/list"
	"context"
	"database/sql/driver"
	"sync"

	"github.com/connerohnesorge/dukdb-go/internal/engine"
)

// StmtCacheConfig holds configuration for statement caching
type StmtCacheConfig struct {
	MaxSize int  // Maximum number of statements to cache
	Enabled bool // Whether caching is enabled
}

// DefaultStmtCacheConfig returns sensible defaults
func DefaultStmtCacheConfig() *StmtCacheConfig {
	return &StmtCacheConfig{
		MaxSize: 100,
		Enabled: true,
	}
}

// StmtCache implements an LRU cache for prepared statements
type StmtCache struct {
	mu      sync.RWMutex
	maxSize int
	items   map[string]*list.Element
	lru     *list.List
	enabled bool
}

type cacheItem struct {
	query string
	stmt  *engine.PreparedStatement
}

// NewStmtCache creates a new statement cache
func NewStmtCache(config *StmtCacheConfig) *StmtCache {
	if config == nil {
		config = DefaultStmtCacheConfig()
	}

	return &StmtCache{
		maxSize: config.MaxSize,
		items:   make(map[string]*list.Element),
		lru:     list.New(),
		enabled: config.Enabled,
	}
}

// Get retrieves a statement from the cache
func (c *StmtCache) Get(query string) (*engine.PreparedStatement, bool) {
	if !c.enabled {
		return nil, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[query]
	if !ok {
		return nil, false
	}

	// Move to front (mark as recently used)
	c.lru.MoveToFront(elem)
	item := elem.Value.(*cacheItem)
	return item.stmt, true
}

// Put adds a statement to the cache
func (c *StmtCache) Put(query string, stmt *engine.PreparedStatement) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already exists
	if elem, ok := c.items[query]; ok {
		c.lru.MoveToFront(elem)
		elem.Value.(*cacheItem).stmt = stmt
		return
	}

	// Add new item
	elem := c.lru.PushFront(&cacheItem{
		query: query,
		stmt:  stmt,
	})
	c.items[query] = elem

	// Evict if necessary
	if c.lru.Len() > c.maxSize {
		c.evictLRU()
	}
}

// evictLRU removes the least recently used item
func (c *StmtCache) evictLRU() {
	elem := c.lru.Back()
	if elem != nil {
		item := elem.Value.(*cacheItem)
		delete(c.items, item.query)
		c.lru.Remove(elem)
		// Note: Pure Go implementation doesn't need explicit cleanup
	}
}

// Clear removes all items from the cache
func (c *StmtCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.lru = list.New()
}

// Len returns the number of items in the cache
func (c *StmtCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lru.Len()
}

// Enable enables the cache
func (c *StmtCache) Enable() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enabled = true
}

// Disable disables the cache and clears it
func (c *StmtCache) Disable() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enabled = false
	c.items = make(map[string]*list.Element)
	c.lru = list.New()
}

// CachedConn wraps a connection with statement caching
type CachedConn struct {
	*Conn
	cache *StmtCache
}

// NewCachedConn creates a new cached connection
func NewCachedConn(conn *Conn, cache *StmtCache) *CachedConn {
	if cache == nil {
		cache = NewStmtCache(nil)
	}
	return &CachedConn{
		Conn:  conn,
		cache: cache,
	}
}

// PrepareContext prepares a statement with caching
func (cc *CachedConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	// Check cache first
	if stmt, ok := cc.cache.Get(query); ok {
		return &Stmt{
			conn:       cc.Conn,
			engineStmt: stmt,
			query:      query,
		}, nil
	}

	// Not in cache, prepare new statement
	stmt, err := cc.Conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	// Add to cache
	if s, ok := stmt.(*Stmt); ok {
		cc.cache.Put(query, s.engineStmt)
	}

	return stmt, nil
}

// Close closes the connection and clears the cache
func (cc *CachedConn) Close() error {
	cc.cache.Clear()
	return cc.Conn.Close()
}