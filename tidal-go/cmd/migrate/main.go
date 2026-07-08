// migrate 是冷热分离迁移工具:把 tasks 表里已完成(completed/dead)的历史数据
// 分批搬迁到 tasks_archive,并从 tasks 删除,使主表瘦身、fetch 恒定快。
//
// 特性:
//   - 分批(默认每批 20000 行),避免大事务锁表;
//   - 幂等 + 可中断续跑:archive 用 INSERT IGNORE,tasks 删除只删已确认进 archive 的;
//   - 每批一个事务(先 INSERT 到 archive 提交,再 DELETE 已归档的),中途断了重跑不会丢/不会重复;
//   - 实时进度输出;
//   - --dry-run 只统计不写。
//
// 用法:
//   migrate -dsn '<DSN>' [-batch 20000] [-dry-run]
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	dsn := flag.String("dsn", "", "MySQL DSN(必填)")
	batch := flag.Int("batch", 20000, "每批搬迁行数")
	dryRun := flag.Bool("dry-run", false, "只统计不实际搬迁")
	flag.Parse()

	if *dsn == "" {
		log.Fatal("必须提供 -dsn")
	}

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	if err := db.Ping(); err != nil {
		log.Fatalf("Ping 失败: %v", err)
	}

	// 前置检查:archive 表必须存在
	var tbl string
	if err := db.QueryRow("SHOW TABLES LIKE 'tasks_archive'").Scan(&tbl); err != nil {
		log.Fatal("tasks_archive 表不存在,请先执行 migrations/001_cold_hot_split.sql 建表")
	}

	// 统计待迁移量
	var total int64
	if err := db.QueryRow("SELECT COUNT(*) FROM tasks WHERE status IN ('completed','dead')").Scan(&total); err != nil {
		log.Fatalf("统计失败: %v", err)
	}
	log.Printf("待迁移(completed/dead)行数: %d", total)
	if total == 0 {
		log.Println("无数据需迁移,完成。")
		return
	}
	if *dryRun {
		log.Println("[dry-run] 仅统计,未做任何改动。")
		return
	}

	// 按 id 升序分批。每批:先把该区间 completed/dead 复制进 archive(INSERT IGNORE 幂等),
	// 再从 tasks 删除"已确实存在于 archive"的这些行(保证不会删掉没搬成功的)。
	start := time.Now()
	var movedTotal int64
	var lastID int64 = 0

	insertSQL := `INSERT IGNORE INTO tasks_archive
	  (id,job_id,track_id,title,artist,album,album_id,track_number,duration,audio_quality,isrc,
	   status,assigned_account_id,retry_count,error_code,error_message,file_size,actual_quality,
	   codec,s3_key,storage_id,completed_at,export_group_idx)
	  SELECT id,job_id,track_id,title,artist,album,album_id,track_number,duration,audio_quality,isrc,
	   status,assigned_account_id,retry_count,error_code,error_message,file_size,actual_quality,
	   codec,s3_key,storage_id,completed_at,export_group_idx
	  FROM tasks
	  WHERE status IN ('completed','dead') AND id > ? ORDER BY id LIMIT ?`

	// 删除:只删这批里已进 archive 的(用 JOIN 确保安全)
	deleteSQL := `DELETE t FROM tasks t JOIN tasks_archive a ON t.id = a.id
	  WHERE t.status IN ('completed','dead') AND t.id > ? AND t.id <= ?`

	for {
		// 找这批的 id 上界(第 batch 个 completed/dead 的 id)
		var upperID sql.NullInt64
		err := db.QueryRow(
			"SELECT MAX(id) FROM (SELECT id FROM tasks WHERE status IN ('completed','dead') AND id > ? ORDER BY id LIMIT ?) x",
			lastID, *batch,
		).Scan(&upperID)
		if err != nil {
			log.Fatalf("取批次上界失败: %v", err)
		}
		if !upperID.Valid {
			break // 没有更多了
		}

		// 1. 复制到 archive
		if _, err := db.Exec(insertSQL, lastID, *batch); err != nil {
			log.Fatalf("INSERT 到 archive 失败(id>%d): %v", lastID, err)
		}
		// 2. 删除已归档的
		res, err := db.Exec(deleteSQL, lastID, upperID.Int64)
		if err != nil {
			log.Fatalf("DELETE 失败(id %d~%d): %v", lastID, upperID.Int64, err)
		}
		n, _ := res.RowsAffected()
		movedTotal += n
		lastID = upperID.Int64

		pct := float64(movedTotal) / float64(total) * 100
		if pct > 100 {
			pct = 100
		}
		elapsed := time.Since(start).Seconds()
		rate := float64(movedTotal) / elapsed
		log.Printf("已迁移 %d/%d (%.1f%%) | 当前id=%d | %.0f 行/秒", movedTotal, total, pct, lastID, rate)

		if n == 0 {
			break // 保险:这批没删到,避免死循环
		}
		// 轻微让步,减轻 DB 压力(生产迁移期间也在被其他业务用)
		time.Sleep(50 * time.Millisecond)
	}

	// 收尾统计
	var remain, archived, tasksLeft int64
	db.QueryRow("SELECT COUNT(*) FROM tasks WHERE status IN ('completed','dead')").Scan(&remain)
	db.QueryRow("SELECT COUNT(*) FROM tasks_archive").Scan(&archived)
	db.QueryRow("SELECT COUNT(*) FROM tasks").Scan(&tasksLeft)

	fmt.Println("========================================")
	log.Printf("✅ 迁移完成,耗时 %.0fs", time.Since(start).Seconds())
	log.Printf("   本次搬迁: %d 行", movedTotal)
	log.Printf("   tasks 剩余 completed/dead: %d (应为 0)", remain)
	log.Printf("   tasks_archive 总量: %d", archived)
	log.Printf("   tasks 主表剩余(活跃任务): %d", tasksLeft)
	if remain > 0 {
		log.Printf("⚠️  仍有 %d 行未迁移,可重跑本工具续迁", remain)
	} else {
		log.Println("   建议下一步(可选): OPTIMIZE TABLE tasks; 回收磁盘空间")
	}
}
