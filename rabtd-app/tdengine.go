package main

import (
	"database/sql"
	"fmt"
	"log"
	// "go/driver-go-3.7.4/taosSql"
	_ "github.com/taosdata/driver-go/v3/taosSql" // 导入驱动
	// "pkg.go.dev/github.com/taosdata/driver-go/v3/taoSql"
)

// initTDengine 初始化TDengine连接，并确保数据库和超级表存在
func initTDengine() (*sql.DB, error) {
    // 连接到默认数据库
	db, err := sql.Open("taosSql", tdEngineDSN)

	if err != nil {
		return nil, fmt.Errorf("error opening TDengine: %v", err)
	}

    // 创建数据库
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", targetDatabase))
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("error creating database: %v", err)
	}

    // 使用目标数据库
	_, err = db.Exec(fmt.Sprintf("USE %s", targetDatabase))
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("error using database: %v", err)
	}

    // 创建超级表 (示例：传感器数据)
	createSuperTableSQL := fmt.Sprintf(`
		CREATE STABLE IF NOT EXISTS %s.%s (
			ts TIMESTAMP,
			temperature FLOAT,
			humidity FLOAT,
			location NCHAR(50),
			sensor_id NCHAR(20)
		) TAGS (
			device_id NCHAR(50)
		)
	`, targetDatabase, superTableName)
	_, err = db.Exec(createSuperTableSQL)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("error creating super table: %v", err)
	}

	log.Println("TDengine initialized successfully.")
	return db, nil
}