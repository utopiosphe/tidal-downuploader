package worker

import (
	"math/rand"
	"sort"
	"sync"
	"time"

	"tidal-go/internal/models"
)

const maxPerAccount = 30

// AccountPool 本地账号池:智能选号 + 冷却规避(对应 Python AccountPool)。
type AccountPool struct {
	mu          sync.Mutex
	accounts    map[int64]models.Account
	cooldowns   map[int64]time.Time // account_id -> 冷却截止
	failCounts  map[int64]int       // 连续 429 次数
	activeTasks map[int64]int       // 当前活跃任务数
}

// NewAccountPool 创建账号池。
func NewAccountPool() *AccountPool {
	return &AccountPool{
		accounts:    make(map[int64]models.Account),
		cooldowns:   make(map[int64]time.Time),
		failCounts:  make(map[int64]int),
		activeTasks: make(map[int64]int),
	}
}

// Sync 同步 server 下发的账号列表。
func (p *AccountPool) Sync(accts []models.Account) {
	p.mu.Lock()
	defer p.mu.Unlock()
	next := make(map[int64]models.Account, len(accts))
	for _, a := range accts {
		next[a.ID] = a
	}
	p.accounts = next
}

// Pick 选择最优账号(最闲 + 不在冷却 + 未超并发),并即选即锁(活跃+1)。
func (p *AccountPool) Pick() (models.Account, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()

	var avail []models.Account
	for id, a := range p.accounts {
		if cd, ok := p.cooldowns[id]; ok && cd.After(now) {
			continue
		}
		if p.activeTasks[id] >= maxPerAccount {
			continue
		}
		avail = append(avail, a)
	}
	if len(avail) == 0 {
		return models.Account{}, false
	}
	// 按活跃数排序,同活跃度随机打散(均匀使用)
	rand.Shuffle(len(avail), func(i, j int) { avail[i], avail[j] = avail[j], avail[i] })
	sort.SliceStable(avail, func(i, j int) bool {
		return p.activeTasks[avail[i].ID] < p.activeTasks[avail[j].ID]
	})
	best := avail[0]
	p.activeTasks[best.ID]++
	return best, true
}

// Release 任务结束,活跃-1。
func (p *AccountPool) Release(id int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.activeTasks[id] > 0 {
		p.activeTasks[id]--
	}
}

// ReportSuccess 成功,重置失败计数。
func (p *AccountPool) ReportSuccess(id int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failCounts[id] = 0
}

// ReportRateLimited 429,指数退避冷却(15s→最多 300s)。
func (p *AccountPool) ReportRateLimited(id int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c := p.failCounts[id] + 1
	p.failCounts[id] = c
	sec := 15 << c // 2^c * 15
	if sec > 300 {
		sec = 300
	}
	p.cooldowns[id] = time.Now().Add(time.Duration(sec) * time.Second)
}

// ReportUnavailable token 过期/封禁,移出池。
func (p *AccountPool) ReportUnavailable(id int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.accounts, id)
}

// Summary 状态摘要。
func (p *AccountPool) Summary() (total, cooling int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	total = len(p.accounts)
	for _, cd := range p.cooldowns {
		if cd.After(now) {
			cooling++
		}
	}
	return
}
