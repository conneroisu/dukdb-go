package driver

import (
	"container/list"
	"context"
	"database/sql/driver"
	"sync"

	"github.com/connerohnesorge/dukdb-go/internal/purego"
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
	mu       sync.RWMutex
	maxSize  int
	items    map[string]*list.Element
	lru      *list.List
	enabled  bool
}

type cacheItem struct {
	query string
	stmt  purego.PreparedStatement
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

// Get retrieves a prepared statement from the cache
func (c *StmtCache) Get(query string) (purego.PreparedStatement, bool) {
	if !c.enabled {
		return 0, false
	}
	
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if elem, exists := c.items[query]; exists {
		// Move to front (most recently used)
		c.lru.MoveToFront(elem)
		item := elem.Value.(*cacheItem)
		return item.stmt, true
	}
	
	return 0, false
}

// Put adds a prepared statement to the cache
func (c *StmtCache) Put(query string, stmt purego.PreparedStatement) {
	if !c.enabled {
		return
	}
	
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Check if already exists
	if elem, exists := c.items[query]; exists {
		// Update and move to front
		c.lru.MoveToFront(elem)
		item := elem.Value.(*cacheItem)
		item.stmt = stmt
		return
	}
	
	// Add new item
	item := &cacheItem{
		query: query,
		stmt:  stmt,
	}
	elem := c.lru.PushFront(item)
	c.items[query] = elem
	
	// Evict if necessary
	if c.lru.Len() > c.maxSize {
		c.evictLRU()
	}
}

// Remove removes a statement from the cache
func (c *StmtCache) Remove(query string) {
	if !c.enabled {
		return
	}
	
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if elem, exists := c.items[query]; exists {
		c.removeElement(elem)
	}
}

// Clear clears all cached statements
func (c *StmtCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.items = make(map[string]*list.Element)
	c.lru.Init()
}

// Size returns the current cache size
func (c *StmtCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	return c.lru.Len()
}

// Stats returns cache statistics
func (c *StmtCache) Stats() StmtCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	return StmtCacheStats{
		Size:    c.lru.Len(),
		MaxSize: c.maxSize,
		Enabled: c.enabled,
	}
}

// StmtCacheStats holds cache statistics
type StmtCacheStats struct {
	Size    int
	MaxSize int
	Enabled bool
}

func (c *StmtCache) evictLRU() {
	if c.lru.Len() == 0 {
		return
	}
	
	elem := c.lru.Back()
	if elem != nil {
		c.removeElement(elem)
	}
}

func (c *StmtCache) removeElement(elem *list.Element) {
	item := elem.Value.(*cacheItem)
	delete(c.items, item.query)
	c.lru.Remove(elem)
}

// CachedConn wraps a connection with statement caching
type CachedConn struct {
	*Conn
	cache *StmtCache
}

// NewCachedConn creates a connection with statement caching
func NewCachedConn(conn *Conn, config *StmtCacheConfig) *CachedConn {
	return &CachedConn{
		Conn:  conn,
		cache: NewStmtCache(config),
	}
}

// PrepareContext prepares a statement with caching
func (cc *CachedConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	// Try to get from cache first
	if cachedStmt, found := cc.cache.Get(query); found {
		return &Stmt{
			conn:   cc.Conn,
			duckdb: cc.duckdb,
			stmt:   cachedStmt,
			query:  query,
		}, nil
	}
	
	// Not in cache, prepare new statement
	stmt, err := cc.duckdb.Prepare(cc.conn, query)
	if err != nil {
		return nil, err
	}
	
	// Add to cache
	cc.cache.Put(query, stmt)
	
	return &Stmt{
		conn:   cc.Conn,
		duckdb: cc.duckdb,
		stmt:   stmt,
		query:  query,
	}, nil
}

// CacheStats returns statement cache statistics
func (cc *CachedConn) CacheStats() StmtCacheStats {
	return cc.cache.Stats()
}

// ClearCache clears the statement cache
func (cc *CachedConn) ClearCache() {
	cc.cache.Clear()
}