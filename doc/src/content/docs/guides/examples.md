# Examples

Complete code examples demonstrating various features of the **dukdb-go** driver.

## Basic Usage

### Simple Query Example

```go
package main

import (
    "database/sql"
    "fmt"
    "log"
    
    _ "github.com/connerohnesorge/dukdb-go/driver"
)

func main() {
    // Open database connection
    db, err := sql.Open("duckdb", ":memory:")
    if err != nil {
        log.Fatal("Failed to open database:", err)
    }
    defer db.Close()
    
    // Test connection
    if err := db.Ping(); err != nil {
        log.Fatal("Failed to ping database:", err)
    }
    
    // Create a table
    _, err = db.Exec(`
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            age INTEGER
        )
    `)
    if err != nil {
        log.Fatal("Failed to create table:", err)
    }
    
    // Insert data
    _, err = db.Exec(`
        INSERT INTO users (name, email, age) VALUES 
        ('Alice Johnson', 'alice@example.com', 30),
        ('Bob Smith', 'bob@example.com', 25),
        ('Charlie Brown', 'charlie@example.com', 35)
    `)
    if err != nil {
        log.Fatal("Failed to insert data:", err)
    }
    
    // Query data
    rows, err := db.Query("SELECT id, name, email, age FROM users ORDER BY age")
    if err != nil {
        log.Fatal("Failed to query data:", err)
    }
    defer rows.Close()
    
    fmt.Println("Users:")
    for rows.Next() {
        var id, age int
        var name, email string
        
        err := rows.Scan(&id, &name, &email, &age)
        if err != nil {
            log.Fatal("Failed to scan row:", err)
        }
        
        fmt.Printf("  %d: %s (%s) - Age: %d\n", id, name, email, age)
    }
    
    if err := rows.Err(); err != nil {
        log.Fatal("Row iteration error:", err)
    }
}
```

## Prepared Statements

### Bulk Insert with Prepared Statements

```go
package main

import (
    "database/sql"
    "fmt"
    "log"
    "time"
    
    _ "github.com/connerohnesorge/dukdb-go/driver"
)

type Order struct {
    ID        int
    ProductID int
    Quantity  int
    Price     float64
    OrderDate time.Time
}

func main() {
    db, err := sql.Open("duckdb", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Create orders table
    _, err = db.Exec(`
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            quantity INTEGER,
            price DECIMAL(10,2),
            order_date TIMESTAMP
        )
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Prepare statement for bulk inserts
    stmt, err := db.Prepare(`
        INSERT INTO orders (product_id, quantity, price, order_date) 
        VALUES (?, ?, ?, ?)
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer stmt.Close()
    
    // Sample orders
    orders := []Order{
        {ProductID: 101, Quantity: 2, Price: 29.99, OrderDate: time.Now()},
        {ProductID: 102, Quantity: 1, Price: 49.99, OrderDate: time.Now().Add(-time.Hour)},
        {ProductID: 103, Quantity: 5, Price: 9.99, OrderDate: time.Now().Add(-2*time.Hour)},
        {ProductID: 101, Quantity: 1, Price: 29.99, OrderDate: time.Now().Add(-3*time.Hour)},
    }
    
    // Insert orders using prepared statement
    for _, order := range orders {
        result, err := stmt.Exec(
            order.ProductID, 
            order.Quantity, 
            order.Price, 
            order.OrderDate,
        )
        if err != nil {
            log.Fatal(err)
        }
        
        id, _ := result.LastInsertId()
        fmt.Printf("Inserted order with ID: %d\n", id)
    }
    
    // Query results with aggregation
    rows, err := db.Query(`
        SELECT 
            product_id,
            COUNT(*) as order_count,
            SUM(quantity) as total_quantity,
            SUM(price * quantity) as total_revenue
        FROM orders 
        GROUP BY product_id 
        ORDER BY total_revenue DESC
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    fmt.Println("\nOrder Summary:")
    for rows.Next() {
        var productID, orderCount, totalQuantity int
        var totalRevenue float64
        
        err := rows.Scan(&productID, &orderCount, &totalQuantity, &totalRevenue)
        if err != nil {
            log.Fatal(err)
        }
        
        fmt.Printf("Product %d: %d orders, %d items, $%.2f revenue\n",
            productID, orderCount, totalQuantity, totalRevenue)
    }
}
```

## Transactions

### Transaction Example with Rollback

```go
package main

import (
    "database/sql"
    "fmt"
    "log"
    
    _ "github.com/connerohnesorge/dukdb-go/driver"
)

func main() {
    db, err := sql.Open("duckdb", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Create accounts table
    _, err = db.Exec(`
        CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            balance DECIMAL(10,2)
        )
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Insert initial accounts
    _, err = db.Exec(`
        INSERT INTO accounts (name, balance) VALUES 
        ('Alice', 1000.00),
        ('Bob', 500.00)
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Function to transfer money between accounts
    transfer := func(fromID, toID int, amount float64) error {
        tx, err := db.Begin()
        if err != nil {
            return err
        }
        defer tx.Rollback() // Rollback if not committed
        
        // Check from account balance
        var fromBalance float64
        err = tx.QueryRow("SELECT balance FROM accounts WHERE id = ?", fromID).Scan(&fromBalance)
        if err != nil {
            return fmt.Errorf("failed to get from account balance: %w", err)
        }
        
        if fromBalance < amount {
            return fmt.Errorf("insufficient funds: balance %.2f, transfer %.2f", fromBalance, amount)
        }
        
        // Debit from account
        _, err = tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ?", amount, fromID)
        if err != nil {
            return fmt.Errorf("failed to debit account: %w", err)
        }
        
        // Credit to account
        _, err = tx.Exec("UPDATE accounts SET balance = balance + ? WHERE id = ?", amount, toID)
        if err != nil {
            return fmt.Errorf("failed to credit account: %w", err)
        }
        
        // Commit transaction
        return tx.Commit()
    }
    
    // Function to display account balances
    showBalances := func() {
        rows, err := db.Query("SELECT id, name, balance FROM accounts ORDER BY id")
        if err != nil {
            log.Fatal(err)
        }
        defer rows.Close()
        
        fmt.Println("Account Balances:")
        for rows.Next() {
            var id int
            var name string
            var balance float64
            
            err := rows.Scan(&id, &name, &balance)
            if err != nil {
                log.Fatal(err)
            }
            
            fmt.Printf("  %d: %s - $%.2f\n", id, name, balance)
        }
        fmt.Println()
    }
    
    fmt.Println("Initial balances:")
    showBalances()
    
    // Successful transfer
    fmt.Println("Transferring $200 from Alice (1) to Bob (2)...")
    err = transfer(1, 2, 200.00)
    if err != nil {
        fmt.Printf("Transfer failed: %v\n", err)
    } else {
        fmt.Println("Transfer successful!")
    }
    showBalances()
    
    // Failed transfer (insufficient funds)
    fmt.Println("Attempting to transfer $1000 from Bob (2) to Alice (1)...")
    err = transfer(2, 1, 1000.00)
    if err != nil {
        fmt.Printf("Transfer failed: %v\n", err)
    } else {
        fmt.Println("Transfer successful!")
    }
    showBalances()
}
```

## Complex Types

### Working with Lists, Structs, and Maps

```go
package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "log"
    
    _ "github.com/connerohnesorge/dukdb-go/driver"
    "github.com/connerohnesorge/dukdb-go/internal/types"
)

func main() {
    db, err := sql.Open("duckdb", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Create table with complex types
    _, err = db.Exec(`
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            tags LIST(VARCHAR),
            metadata STRUCT(category VARCHAR, weight DOUBLE, dimensions STRUCT(length DOUBLE, width DOUBLE, height DOUBLE)),
            attributes MAP(VARCHAR, VARCHAR)
        )
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Insert data with complex types
    _, err = db.Exec(`
        INSERT INTO products (name, tags, metadata, attributes) VALUES 
        (
            'Laptop',
            ['electronics', 'computer', 'portable'],
            {'category': 'Electronics', 'weight': 2.5, 'dimensions': {'length': 35.0, 'width': 25.0, 'height': 2.0}},
            map(['brand', 'color', 'warranty'], ['Dell', 'Black', '2 years'])
        ),
        (
            'Coffee Mug',
            ['kitchen', 'ceramic', 'dishwasher-safe'],
            {'category': 'Kitchen', 'weight': 0.3, 'dimensions': {'length': 10.0, 'width': 10.0, 'height': 12.0}},
            map(['material', 'color', 'capacity'], ['Ceramic', 'White', '350ml'])
        )
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Query and process complex types
    rows, err := db.Query(`
        SELECT 
            id, 
            name, 
            tags, 
            metadata, 
            attributes,
            tags[1] as first_tag,
            metadata.category as category,
            metadata.dimensions.length as length,
            attributes['brand'] as brand
        FROM products
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    fmt.Println("Products with Complex Types:")
    for rows.Next() {
        var id int
        var name, tagsJSON, metadataJSON, attributesJSON string
        var firstTag, category, brand sql.NullString
        var length sql.NullFloat64
        
        err := rows.Scan(&id, &name, &tagsJSON, &metadataJSON, &attributesJSON,
            &firstTag, &category, &length, &brand)
        if err != nil {
            log.Fatal(err)
        }
        
        fmt.Printf("\nProduct %d: %s\n", id, name)
        
        // Parse tags (LIST)
        var tags []string
        if err := json.Unmarshal([]byte(tagsJSON), &tags); err == nil {
            fmt.Printf("  Tags: %v\n", tags)
        }
        
        // Parse metadata (STRUCT)
        var metadata map[string]interface{}
        if err := json.Unmarshal([]byte(metadataJSON), &metadata); err == nil {
            fmt.Printf("  Category: %v\n", metadata["category"])
            fmt.Printf("  Weight: %v kg\n", metadata["weight"])
            
            if dims, ok := metadata["dimensions"].(map[string]interface{}); ok {
                fmt.Printf("  Dimensions: %.1fx%.1fx%.1f cm\n", 
                    dims["length"], dims["width"], dims["height"])
            }
        }
        
        // Parse attributes (MAP)
        var attributes map[string]string
        if err := json.Unmarshal([]byte(attributesJSON), &attributes); err == nil {
            fmt.Printf("  Attributes: %v\n", attributes)
        }
        
        // Show extracted values
        if firstTag.Valid {
            fmt.Printf("  First Tag: %s\n", firstTag.String)
        }
        if category.Valid {
            fmt.Printf("  Category (extracted): %s\n", category.String)
        }
        if length.Valid {
            fmt.Printf("  Length (extracted): %.1f cm\n", length.Float64)
        }
        if brand.Valid {
            fmt.Printf("  Brand (extracted): %s\n", brand.String)
        }
    }
    
    // Example using helper functions
    fmt.Println("\n\nUsing Helper Functions:")
    
    // Create complex types using helpers
    newTags := types.NewList([]string{"books", "fiction", "bestseller"})
    newMetadata := types.NewStruct(map[string]interface{}{
        "category": "Books",
        "weight":   0.5,
        "dimensions": map[string]interface{}{
            "length": 20.0,
            "width":  15.0,
            "height": 3.0,
        },
    })
    newAttributes := types.NewMap(map[string]string{
        "author":    "Jane Doe",
        "publisher": "Example Press",
        "isbn":      "978-1234567890",
    })
    
    _, err = db.Exec(`
        INSERT INTO products (name, tags, metadata, attributes) VALUES (?, ?, ?, ?)
    `, "Novel", newTags.String(), newMetadata.String(), newAttributes.String())
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Println("Inserted new product using helper functions!")
}
```

## Analytics Example

### Data Analysis with Window Functions

```go
package main

import (
    "database/sql"
    "fmt"
    "log"
    "math/rand"
    "time"
    
    _ "github.com/connerohnesorge/dukdb-go/driver"
)

func main() {
    db, err := sql.Open("duckdb", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Create sales data table
    _, err = db.Exec(`
        CREATE TABLE sales (
            id INTEGER PRIMARY KEY,
            sales_date DATE,
            region VARCHAR,
            product VARCHAR,
            sales_amount DECIMAL(10,2),
            units_sold INTEGER
        )
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Generate sample sales data
    regions := []string{"North", "South", "East", "West"}
    products := []string{"Product A", "Product B", "Product C"}
    
    stmt, err := db.Prepare(`
        INSERT INTO sales (sales_date, region, product, sales_amount, units_sold) 
        VALUES (?, ?, ?, ?, ?)
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer stmt.Close()
    
    rand.Seed(time.Now().UnixNano())
    startDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
    
    for i := 0; i < 1000; i++ {
        saleDate := startDate.AddDate(0, 0, rand.Intn(365))
        region := regions[rand.Intn(len(regions))]
        product := products[rand.Intn(len(products))]
        salesAmount := float64(rand.Intn(5000)+500) / 100 // $5.00 to $55.00
        unitsSold := rand.Intn(20) + 1
        
        _, err := stmt.Exec(saleDate, region, product, salesAmount, unitsSold)
        if err != nil {
            log.Fatal(err)
        }
    }
    
    fmt.Println("Generated 1000 sample sales records")
    
    // Analytics queries
    
    // 1. Monthly sales summary with running totals
    fmt.Println("\n1. Monthly Sales Summary with Running Totals:")
    rows, err := db.Query(`
        SELECT 
            DATE_TRUNC('month', sales_date) as month,
            COUNT(*) as transaction_count,
            SUM(sales_amount) as monthly_sales,
            SUM(units_sold) as monthly_units,
            SUM(SUM(sales_amount)) OVER (ORDER BY DATE_TRUNC('month', sales_date)) as running_total
        FROM sales 
        GROUP BY DATE_TRUNC('month', sales_date)
        ORDER BY month
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    for rows.Next() {
        var month time.Time
        var transactionCount, monthlyUnits int
        var monthlySales, runningTotal float64
        
        err := rows.Scan(&month, &transactionCount, &monthlySales, &monthlyUnits, &runningTotal)
        if err != nil {
            log.Fatal(err)
        }
        
        fmt.Printf("  %s: %d transactions, $%.2f sales, %d units, running total: $%.2f\n",
            month.Format("2006-01"), transactionCount, monthlySales, monthlyUnits, runningTotal)
    }
    
    // 2. Top performing regions with rankings
    fmt.Println("\n2. Regional Performance Rankings:")
    rows, err = db.Query(`
        SELECT 
            region,
            COUNT(*) as transaction_count,
            SUM(sales_amount) as total_sales,
            AVG(sales_amount) as avg_sale,
            RANK() OVER (ORDER BY SUM(sales_amount) DESC) as sales_rank,
            PERCENT_RANK() OVER (ORDER BY SUM(sales_amount)) as percentile_rank
        FROM sales 
        GROUP BY region
        ORDER BY total_sales DESC
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    for rows.Next() {
        var region string
        var transactionCount, salesRank int
        var totalSales, avgSale, percentileRank float64
        
        err := rows.Scan(&region, &transactionCount, &totalSales, &avgSale, &salesRank, &percentileRank)
        if err != nil {
            log.Fatal(err)
        }
        
        fmt.Printf("  %s: Rank %d, $%.2f total (%.0f%% percentile), $%.2f avg, %d transactions\n",
            region, salesRank, totalSales, percentileRank*100, avgSale, transactionCount)
    }
    
    // 3. Product performance with lag analysis
    fmt.Println("\n3. Product Performance with Month-over-Month Growth:")
    rows, err = db.Query(`
        WITH monthly_product_sales AS (
            SELECT 
                DATE_TRUNC('month', sales_date) as month,
                product,
                SUM(sales_amount) as monthly_sales
            FROM sales 
            GROUP BY DATE_TRUNC('month', sales_date), product
        )
        SELECT 
            month,
            product,
            monthly_sales,
            LAG(monthly_sales) OVER (PARTITION BY product ORDER BY month) as prev_month_sales,
            monthly_sales - LAG(monthly_sales) OVER (PARTITION BY product ORDER BY month) as mom_change,
            CASE 
                WHEN LAG(monthly_sales) OVER (PARTITION BY product ORDER BY month) IS NOT NULL 
                THEN (monthly_sales - LAG(monthly_sales) OVER (PARTITION BY product ORDER BY month)) / 
                     LAG(monthly_sales) OVER (PARTITION BY product ORDER BY month) * 100
                ELSE NULL 
            END as mom_change_percent
        FROM monthly_product_sales
        WHERE month >= '2024-02-01'  -- Skip first month (no previous data)
        ORDER BY month, product
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    for rows.Next() {
        var month time.Time
        var product string
        var monthlySales float64
        var prevMonthSales, momChange, momChangePercent sql.NullFloat64
        
        err := rows.Scan(&month, &product, &monthlySales, &prevMonthSales, &momChange, &momChangePercent)
        if err != nil {
            log.Fatal(err)
        }
        
        fmt.Printf("  %s %s: $%.2f", month.Format("2006-01"), product, monthlySales)
        if momChange.Valid {
            fmt.Printf(" (change: $%.2f", momChange.Float64)
            if momChangePercent.Valid {
                fmt.Printf(", %.1f%%", momChangePercent.Float64)
            }
            fmt.Printf(")")
        }
        fmt.Println()
    }
    
    // 4. Sales distribution analysis
    fmt.Println("\n4. Sales Distribution Analysis:")
    rows, err = db.Query(`
        SELECT 
            region,
            product,
            COUNT(*) as transaction_count,
            SUM(sales_amount) as total_sales,
            AVG(sales_amount) as avg_sale,
            STDDEV(sales_amount) as stddev_sale,
            MIN(sales_amount) as min_sale,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sales_amount) as median_sale,
            MAX(sales_amount) as max_sale
        FROM sales 
        GROUP BY region, product
        ORDER BY region, total_sales DESC
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    for rows.Next() {
        var region, product string
        var transactionCount int
        var totalSales, avgSale, stddevSale, minSale, medianSale, maxSale float64
        
        err := rows.Scan(&region, &product, &transactionCount, &totalSales, 
            &avgSale, &stddevSale, &minSale, &medianSale, &maxSale)
        if err != nil {
            log.Fatal(err)
        }
        
        fmt.Printf("  %s - %s: %d txns, $%.2f total, $%.2f avg ±%.2f, range: $%.2f-$%.2f, median: $%.2f\n",
            region, product, transactionCount, totalSales, avgSale, stddevSale, minSale, maxSale, medianSale)
    }
}
```

## Context and Cancellation

### Context-Aware Operations

```go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "log"
    "time"
    
    _ "github.com/connerohnesorge/dukdb-go/driver"
)

func main() {
    db, err := sql.Open("duckdb", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Create a large table for demonstration
    _, err = db.Exec(`
        CREATE TABLE large_data AS 
        SELECT 
            i as id,
            'data_' || i as value,
            random() as random_value
        FROM range(1000000) t(i)
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Println("Created large table with 1M rows")
    
    // Example 1: Query with timeout
    fmt.Println("\n1. Query with 5-second timeout:")
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    start := time.Now()
    rows, err := db.QueryContext(ctx, `
        SELECT COUNT(*), AVG(random_value), STDDEV(random_value)
        FROM large_data
    `)
    if err != nil {
        if err == context.DeadlineExceeded {
            fmt.Printf("Query timed out after %v\n", time.Since(start))
        } else {
            log.Printf("Query error: %v\n", err)
        }
    } else {
        defer rows.Close()
        
        if rows.Next() {
            var count int
            var avg, stddev float64
            err := rows.Scan(&count, &avg, &stddev)
            if err != nil {
                log.Fatal(err)
            }
            
            fmt.Printf("Statistics computed in %v:\n", time.Since(start))
            fmt.Printf("  Count: %d\n", count)
            fmt.Printf("  Average: %.6f\n", avg)
            fmt.Printf("  Std Dev: %.6f\n", stddev)
        }
    }
    
    // Example 2: Cancellable long-running operation
    fmt.Println("\n2. Cancellable operation (cancelled after 2 seconds):")
    ctx2, cancel2 := context.WithCancel(context.Background())
    
    // Cancel after 2 seconds
    go func() {
        time.Sleep(2 * time.Second)
        fmt.Println("Cancelling operation...")
        cancel2()
    }()
    
    start = time.Now()
    rows, err = db.QueryContext(ctx2, `
        SELECT id, value, random_value 
        FROM large_data 
        WHERE random_value > 0.5
        ORDER BY random_value DESC
    `)
    if err != nil {
        if err == context.Canceled {
            fmt.Printf("Operation cancelled after %v\n", time.Since(start))
        } else {
            log.Printf("Query error: %v\n", err)
        }
    } else {
        defer rows.Close()
        
        count := 0
        for rows.Next() {
            // Check for cancellation periodically
            select {
            case <-ctx2.Done():
                fmt.Printf("Processing cancelled after %d rows in %v\n", count, time.Since(start))
                return
            default:
            }
            
            var id int
            var value string
            var randomValue float64
            
            err := rows.Scan(&id, &value, &randomValue)
            if err != nil {
                log.Fatal(err)
            }
            
            count++
            if count%10000 == 0 {
                fmt.Printf("  Processed %d rows...\n", count)
            }
        }
        
        fmt.Printf("Processed %d rows in %v\n", count, time.Since(start))
    }
    
    // Example 3: Transaction with context
    fmt.Println("\n3. Transaction with context:")
    ctx3, cancel3 := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel3()
    
    tx, err := db.BeginTx(ctx3, nil)
    if err != nil {
        log.Fatal(err)
    }
    defer tx.Rollback()
    
    start = time.Now()
    
    // Perform multiple operations in transaction
    _, err = tx.ExecContext(ctx3, `
        CREATE TEMP TABLE temp_summary AS
        SELECT 
            CASE 
                WHEN random_value < 0.3 THEN 'Low'
                WHEN random_value < 0.7 THEN 'Medium'
                ELSE 'High'
            END as category,
            COUNT(*) as count,
            AVG(random_value) as avg_value
        FROM large_data
        GROUP BY category
    `)
    if err != nil {
        if err == context.DeadlineExceeded {
            fmt.Printf("Transaction timed out during CREATE\n")
        } else {
            log.Printf("CREATE error: %v\n", err)
        }
        return
    }
    
    rows, err = tx.QueryContext(ctx3, "SELECT category, count, avg_value FROM temp_summary ORDER BY avg_value")
    if err != nil {
        if err == context.DeadlineExceeded {
            fmt.Printf("Transaction timed out during SELECT\n")
        } else {
            log.Printf("SELECT error: %v\n", err)
        }
        return
    }
    defer rows.Close()
    
    fmt.Printf("Transaction completed in %v:\n", time.Since(start))
    for rows.Next() {
        var category string
        var count int
        var avgValue float64
        
        err := rows.Scan(&category, &count, &avgValue)
        if err != nil {
            log.Fatal(err)
        }
        
        fmt.Printf("  %s: %d records, avg %.6f\n", category, count, avgValue)
    }
    
    err = tx.Commit()
    if err != nil {
        log.Printf("Commit error: %v\n", err)
    } else {
        fmt.Println("Transaction committed successfully")
    }
}
```

## File Operations

### Working with CSV and Parquet Files

```go
package main

import (
    "database/sql"
    "fmt"
    "log"
    "os"
    
    _ "github.com/connerohnesorge/dukdb-go/driver"
)

func main() {
    db, err := sql.Open("duckdb", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Create sample CSV file
    csvContent := `id,name,age,city,salary
1,Alice Johnson,30,New York,75000
2,Bob Smith,25,Los Angeles,68000
3,Charlie Brown,35,Chicago,82000
4,Diana Prince,28,Boston,71000
5,Eve Wilson,32,Seattle,79000
6,Frank Miller,29,Austin,73000
7,Grace Lee,31,Portland,76000
8,Henry Davis,27,Denver,69000
9,Ivy Chen,33,San Francisco,89000
10,Jack Taylor,26,Miami,67000`
    
    err = os.WriteFile("/tmp/employees.csv", []byte(csvContent), 0644)
    if err != nil {
        log.Fatal(err)
    }
    defer os.Remove("/tmp/employees.csv")
    
    fmt.Println("Created sample CSV file")
    
    // Read CSV directly with DuckDB
    fmt.Println("\n1. Reading CSV file directly:")
    rows, err := db.Query("SELECT * FROM '/tmp/employees.csv'")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    columns, _ := rows.Columns()
    fmt.Printf("Columns: %v\n", columns)
    
    count := 0
    for rows.Next() && count < 3 {
        var id int
        var name, city string
        var age int
        var salary float64
        
        err := rows.Scan(&id, &name, &age, &city, &salary)
        if err != nil {
            log.Fatal(err)
        }
        
        fmt.Printf("  %d: %s (%d) from %s, salary: $%.0f\n", id, name, age, city, salary)
        count++
    }
    fmt.Println("  ...")
    
    // Load CSV into table
    fmt.Println("\n2. Loading CSV into table:")
    _, err = db.Exec(`
        CREATE TABLE employees AS 
        SELECT * FROM '/tmp/employees.csv'
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Analyze the data
    rows, err = db.Query(`
        SELECT 
            COUNT(*) as employee_count,
            AVG(age) as avg_age,
            AVG(salary) as avg_salary,
            MIN(salary) as min_salary,
            MAX(salary) as max_salary
        FROM employees
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    if rows.Next() {
        var employeeCount int
        var avgAge, avgSalary, minSalary, maxSalary float64
        
        err := rows.Scan(&employeeCount, &avgAge, &avgSalary, &minSalary, &maxSalary)
        if err != nil {
            log.Fatal(err)
        }
        
        fmt.Printf("Dataset Analysis:\n")
        fmt.Printf("  Employees: %d\n", employeeCount)
        fmt.Printf("  Average age: %.1f years\n", avgAge)
        fmt.Printf("  Average salary: $%.0f\n", avgSalary)
        fmt.Printf("  Salary range: $%.0f - $%.0f\n", minSalary, maxSalary)
    }
    
    // Export to different formats
    fmt.Println("\n3. Exporting data:")
    
    // Export to CSV with custom options
    _, err = db.Exec(`
        COPY (
            SELECT name, age, city, salary 
            FROM employees 
            WHERE salary > 70000
            ORDER BY salary DESC
        ) TO '/tmp/high_earners.csv' (HEADER, DELIMITER ',')
    `)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("  Exported high earners to CSV")
    
    // Export to Parquet
    _, err = db.Exec(`
        COPY employees TO '/tmp/employees.parquet' (FORMAT PARQUET)
    `)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("  Exported all employees to Parquet")
    
    // Read back from Parquet
    fmt.Println("\n4. Reading from Parquet file:")
    rows, err = db.Query(`
        SELECT city, COUNT(*) as employee_count, AVG(salary) as avg_salary
        FROM '/tmp/employees.parquet'
        GROUP BY city
        ORDER BY avg_salary DESC
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    fmt.Println("  Salary by city:")
    for rows.Next() {
        var city string
        var employeeCount int
        var avgSalary float64
        
        err := rows.Scan(&city, &employeeCount, &avgSalary)
        if err != nil {
            log.Fatal(err)
        }
        
        fmt.Printf("    %s: %d employees, avg $%.0f\n", city, employeeCount, avgSalary)
    }
    
    // Clean up exported files
    os.Remove("/tmp/high_earners.csv")
    os.Remove("/tmp/employees.parquet")
    
    // Demonstrate JSON operations
    fmt.Println("\n5. Working with JSON data:")
    
    // Create table with JSON column
    _, err = db.Exec(`
        CREATE TABLE products (
            id INTEGER,
            name VARCHAR,
            details VARCHAR  -- JSON as string
        )
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Insert JSON data
    _, err = db.Exec(`
        INSERT INTO products VALUES 
        (1, 'Laptop', '{"brand": "Dell", "model": "XPS", "specs": {"cpu": "Intel i7", "ram": "16GB", "storage": "512GB SSD"}}'),
        (2, 'Phone', '{"brand": "Apple", "model": "iPhone", "specs": {"cpu": "A15", "ram": "6GB", "storage": "128GB"}}'),
        (3, 'Tablet', '{"brand": "Samsung", "model": "Galaxy Tab", "specs": {"cpu": "Snapdragon", "ram": "8GB", "storage": "256GB"}}')
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Query JSON data (DuckDB has built-in JSON functions)
    rows, err = db.Query(`
        SELECT 
            name,
            details,
            json_extract_string(details, '$.brand') as brand,
            json_extract_string(details, '$.specs.cpu') as cpu
        FROM products
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    fmt.Println("  Product details:")
    for rows.Next() {
        var name, details, brand, cpu string
        
        err := rows.Scan(&name, &details, &brand, &cpu)
        if err != nil {
            log.Fatal(err)
        }
        
        fmt.Printf("    %s by %s (CPU: %s)\n", name, brand, cpu)
    }
}
```

## Next Steps

- [API Reference](/reference/api/) - Complete API documentation
- [Performance Guide](/guides/performance/) - Optimization techniques
- [Troubleshooting](/guides/troubleshooting/) - Common issues and solutions