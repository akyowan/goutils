package pool

import (
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Task interface {
	Run()
}

const (
	DefaultPoolCap               = 256 * 1024
	DefaultMaxIdleWorkerDuration = time.Second * 10
)

type Pool struct {
	Cap                   int32         // 最大工作协程数
	MaxIdleWorkerDuration time.Duration // 空闲协程最大生存时间
	Debug                 bool          // 打印Pool状态信息

	workersCount int32
	needStop     bool
	stopCh       chan struct{}
	lock         sync.Mutex
	ready        []*worker
	workerPool   sync.Pool
}

type worker struct {
	lastUserTime time.Time
	ch           chan Task
}

func (p *Pool) Start() {
	if p.stopCh != nil {
		panic("Pool already started")
	}
	if p.Cap <= 0 {
		p.Cap = DefaultPoolCap
	}
	if p.MaxIdleWorkerDuration <= 0 {
		p.MaxIdleWorkerDuration = DefaultMaxIdleWorkerDuration
	}
	p.stopCh = make(chan struct{})
	p.workerPool.New = func() interface{} {
		return &worker{
			ch: make(chan Task, 1),
		}
	}
	if p.Debug {
		go p.stats()
	}
	go p.clean()
}

func (p *Pool) Put(t Task) bool {
	w := p.getWorker()
	if w == nil {
		return false
	}
	w.ch <- t
	return true
}

func (p *Pool) Stop() {
	if p.stopCh == nil {
		panic("Pool wasn't started")
	}
	close(p.stopCh)
	p.stopCh = nil

	p.lock.Lock()
	defer p.lock.Unlock()
	for i := range p.ready {
		p.ready[i].ch <- nil
		p.ready[i] = nil
	}
	p.ready = p.ready[:0]
	p.needStop = true
}

func (p *Pool) clean() {
	ticker := time.NewTicker(p.MaxIdleWorkerDuration)
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			curTime := time.Now()
			var (
				tmp []*worker
				i   = 0
			)
			p.lock.Lock()
			for ; i < len(p.ready) && curTime.Sub(p.ready[i].lastUserTime) >= p.MaxIdleWorkerDuration; i++ {
				tmp = append(tmp, p.ready[i])
				p.ready[i] = nil
			}
			p.ready = p.ready[i:]
			p.lock.Unlock()
			for i := range tmp {
				tmp[i].ch <- nil
				tmp[i] = nil
			}
		}
	}
}

func (p *Pool) stats() {
	ticker := time.NewTicker(time.Second * 2)
	max := int32(0)
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.lock.Lock()
			if max <= p.workersCount {
				max = p.workersCount
			}
			log.Printf("Ready:%d Active:%d Goroutines:%d Max:%d", len(p.ready), p.workersCount, runtime.NumGoroutine(), max)
			p.lock.Unlock()
		}
	}
}

func (p *Pool) getWorker() *worker {
	var w *worker
	p.lock.Lock()
	defer p.lock.Unlock()
	n := len(p.ready) - 1
	if n > 0 {
		w = p.ready[n]
		p.ready[n] = nil
		p.ready = p.ready[:n]
		return w
	}

	if p.workersCount >= p.Cap {
		return nil
	}

	vw := p.workerPool.Get()
	if vw == nil {
		return nil
	}
	w = vw.(*worker)
	p.workersCount++
	go func() {
		p.workerRun(w)
		p.workerPool.Put(w)
	}()

	return w

}

func (p *Pool) workerRun(w *worker) {
	defer func() {
		atomic.AddInt32(&p.workersCount, -1)
	}()
	for t := range w.ch {
		if t == nil {
			break
		}
		t.Run()
		t = nil
		if !p.release(w) {
			break
		}
	}
}

func (p *Pool) release(w *worker) bool {
	w.lastUserTime = time.Now()
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.needStop {
		return false
	}
	p.ready = append(p.ready, w)
	return true

}
