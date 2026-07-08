package handlers

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"tidal-go/internal/models"
)

// ListJobs 批次列表。pending 从 jobs 字段推算(不扫大表)。
func (h *Handler) ListJobs(c *gin.Context) {
	var jobs []models.Job
	if err := h.DB.Select(&jobs, "SELECT * FROM jobs ORDER BY id DESC"); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	out := make([]gin.H, 0, len(jobs))
	for _, j := range jobs {
		pending := j.TotalTracks - j.Completed - j.Failed
		if pending < 0 {
			pending = 0
		}
		// 活跃数(主表小)
		var active int
		_ = h.DB.Get(&active,
			"SELECT COUNT(*) FROM tasks WHERE job_id=? AND status IN ('assigned','downloading','uploading')", j.ID)
		// 已下载总大小(从 export_groups 汇总,毫秒级)
		var totalBytes int64
		_ = h.DB.Get(&totalBytes, "SELECT COALESCE(SUM(total_size),0) FROM export_groups WHERE job_id=?", j.ID)
		out = append(out, gin.H{
			"id": j.ID, "name": j.Name, "source_file": j.SourceFile,
			"total_tracks": j.TotalTracks, "completed": j.Completed, "failed": j.Failed,
			"target_quality": j.TargetQuality, "status": j.Status,
			"created_at": j.CreatedAt, "updated_at": j.UpdatedAt,
			"pending_count": pending, "active_count": active, "total_bytes": totalBytes,
		})
	}
	c.JSON(http.StatusOK, out)
}

// GetJob 批次详情。
func (h *Handler) GetJob(c *gin.Context) {
	id := c.Param("id")
	var j models.Job
	if err := h.DB.Get(&j, "SELECT * FROM jobs WHERE id=?", id); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "批次不存在"})
		return
	}
	c.JSON(http.StatusOK, j)
}

// jsonTrack 导入 JSON 时的曲目结构(字段宽松)。
type jsonTrack struct {
	ID           int64           `json:"id"`
	Title        string          `json:"title"`
	Artist       json.RawMessage `json:"artist"`
	Album        json.RawMessage `json:"album"`
	TrackNumber  int             `json:"trackNumber"`
	Duration     int             `json:"duration"`
	AudioQuality string          `json:"audioQuality"`
	ISRC         string          `json:"isrc"`
}

// ImportJSON 导入 JSON 曲目列表创建批次。
func (h *Handler) ImportJSON(c *gin.Context) {
	name := c.PostForm("name")
	quality := c.DefaultPostForm("quality", "LOSSLESS")
	fh, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "缺少文件"})
		return
	}
	f, _ := fh.Open()
	defer f.Close()
	data, _ := io.ReadAll(f)

	var tracks []jsonTrack
	if err := json.Unmarshal(data, &tracks); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "JSON 解析失败: " + err.Error()})
		return
	}
	if name == "" {
		name = fh.Filename
	}

	res, err := h.DB.Exec(
		"INSERT INTO jobs (name, source_file, total_tracks, target_quality, status) VALUES (?,?,?,?,'running')",
		name, fh.Filename, len(tracks), quality)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	jobID, _ := res.LastInsertId()

	// 批量插入(每 5000 一批)
	h.bulkInsertTasks(jobID, quality, tracks)

	c.JSON(http.StatusOK, gin.H{"message": "导入成功", "job_id": jobID, "total_tracks": len(tracks)})
}

func (h *Handler) bulkInsertTasks(jobID int64, quality string, tracks []jsonTrack) {
	const batch = 5000
	buf := make([]jsonTrack, 0, batch)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		var sb strings.Builder
		sb.WriteString("INSERT INTO tasks (job_id,track_id,title,artist,album,album_id,track_number,duration,audio_quality,isrc) VALUES ")
		args := make([]any, 0, len(buf)*10)
		for i, t := range buf {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString("(?,?,?,?,?,?,?,?,?,?)")
			artist := parseName(t.Artist)
			album, albumID := parseAlbum(t.Album)
			aq := t.AudioQuality
			if aq == "" {
				aq = quality
			}
			args = append(args, jobID, t.ID, defStr(t.Title, "Unknown"), artist, album, albumID,
				t.TrackNumber, t.Duration, aq, t.ISRC)
		}
		_, _ = h.DB.Exec(sb.String(), args...)
		buf = buf[:0]
	}
	for _, t := range tracks {
		buf = append(buf, t)
		if len(buf) >= batch {
			flush()
		}
	}
	flush()
}

// ImportCSV 流式导入超大 CSV(只需 id + isrc 列)。
func (h *Handler) ImportCSV(c *gin.Context) {
	name := c.PostForm("name")
	quality := c.DefaultPostForm("quality", "LOSSLESS")
	fh, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "缺少文件"})
		return
	}
	if name == "" {
		name = fh.Filename
	}

	res, _ := h.DB.Exec(
		"INSERT INTO jobs (name, source_file, total_tracks, target_quality, status) VALUES (?,?,0,?,'running')",
		name, fh.Filename, quality)
	jobID, _ := res.LastInsertId()

	f, _ := fh.Open()
	defer f.Close()
	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "CSV 为空"})
		return
	}
	idCol, isrcCol := -1, -1
	for i, col := range header {
		switch strings.ToLower(strings.TrimSpace(col)) {
		case "id":
			idCol = i
		case "isrc":
			isrcCol = i
		}
	}
	if idCol < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "CSV 缺少 id 列"})
		return
	}

	total := 0
	const batch = 5000
	var sb strings.Builder
	var args []any
	n := 0
	flush := func() {
		if n == 0 {
			return
		}
		_, _ = h.DB.Exec(sb.String(), args...)
		sb.Reset()
		args = args[:0]
		n = 0
	}
	begin := func() {
		sb.WriteString("INSERT INTO tasks (job_id,track_id,title,artist,album,album_id,track_number,duration,audio_quality,isrc) VALUES ")
	}
	begin()
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil || idCol >= len(rec) {
			continue
		}
		idStr := strings.TrimSpace(rec[idCol])
		tid, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			continue
		}
		isrc := ""
		if isrcCol >= 0 && isrcCol < len(rec) {
			isrc = strings.TrimSpace(rec[isrcCol])
		}
		if n > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?,?,?,?,?,?,?,?)")
		args = append(args, jobID, tid, "Unknown", "Unknown", "Unknown", 0, 0, 0, quality, isrc)
		n++
		total++
		if n >= batch {
			flush()
			begin()
		}
	}
	flush()

	_, _ = h.DB.Exec("UPDATE jobs SET total_tracks=? WHERE id=?", total, jobID)
	c.JSON(http.StatusOK, gin.H{"message": "导入成功", "job_id": jobID, "total_tracks": total})
}

// RetryFailed 把该批次的 dead 任务(在归档表)恢复为 pending(拉回活跃表)。
func (h *Handler) RetryFailed(c *gin.Context) {
	id := c.Param("id")
	// 从 archive 把 dead 拉回 tasks(status=pending),并从 archive 删除
	tx, err := h.DB.Beginx()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer tx.Rollback()

	res, _ := tx.Exec(
		"INSERT INTO tasks (id,job_id,track_id,title,artist,album,album_id,track_number,duration,audio_quality,isrc,status,max_retries) "+
			"SELECT id,job_id,track_id,title,artist,album,album_id,track_number,duration,audio_quality,isrc,'pending',3 "+
			"FROM tasks_archive WHERE job_id=? AND status='dead'", id)
	n, _ := res.RowsAffected()
	_, _ = tx.Exec("DELETE FROM tasks_archive WHERE job_id=? AND status='dead'", id)
	// 回退 jobs.failed 计数,批次重新 running
	_, _ = tx.Exec("UPDATE jobs SET failed=GREATEST(failed-?,0), status='running' WHERE id=?", n, id)
	if err := tx.Commit(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "已重试失败任务", "count": n})
}

// PauseJob / ResumeJob 暂停/恢复批次(改 status,fetch 只取 running)。
func (h *Handler) PauseJob(c *gin.Context) {
	_, _ = h.DB.Exec("UPDATE jobs SET status='paused' WHERE id=? AND status='running'", c.Param("id"))
	c.JSON(http.StatusOK, gin.H{"message": "已暂停"})
}

func (h *Handler) ResumeJob(c *gin.Context) {
	_, _ = h.DB.Exec("UPDATE jobs SET status='running' WHERE id=? AND status='paused'", c.Param("id"))
	c.JSON(http.StatusOK, gin.H{"message": "已恢复"})
}

// GetJobTasks 批次任务列表(分页,活跃表 + 归档表联合)。
func (h *Handler) GetJobTasks(c *gin.Context) {
	id := c.Param("id")
	status := c.Query("status")
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "50"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 50
	}
	offset := (page - 1) * pageSize

	// completed/dead 查归档表,其余查活跃表
	var tasks []models.Task
	var table string
	if status == "completed" || status == "dead" {
		table = "tasks_archive"
	} else {
		table = "tasks"
	}
	q := "SELECT * FROM " + table + " WHERE job_id=?"
	args := []any{id}
	if status != "" {
		q += " AND status=?"
		args = append(args, status)
	}
	q += " ORDER BY id LIMIT ? OFFSET ?"
	args = append(args, pageSize, offset)
	_ = h.DB.Select(&tasks, q, args...)
	c.JSON(http.StatusOK, gin.H{"tasks": tasks, "page": page, "page_size": pageSize})
}

// ---- helpers ----

func parseName(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "Unknown"
	}
	var obj struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(raw, &obj) == nil && obj.Name != "" {
		return obj.Name
	}
	var s string
	if json.Unmarshal(raw, &s) == nil && s != "" {
		return s
	}
	return "Unknown"
}

func parseAlbum(raw json.RawMessage) (string, int64) {
	if len(raw) == 0 {
		return "Unknown", 0
	}
	var obj struct {
		Title string `json:"title"`
		ID    int64  `json:"id"`
	}
	if json.Unmarshal(raw, &obj) == nil && obj.Title != "" {
		return obj.Title, obj.ID
	}
	var s string
	if json.Unmarshal(raw, &s) == nil && s != "" {
		return s, 0
	}
	return "Unknown", 0
}

func defStr(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
