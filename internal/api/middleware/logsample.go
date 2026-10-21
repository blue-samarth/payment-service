package middleware

import (
	"sync"
	"time"
)

const logSampleInterval = time.Second

type logSampler struct {
	interval   time.Duration
	mu         sync.Mutex
	next       time.Time
	suppressed int
}

func newLogSampler(interval time.Duration) *logSampler {
	return &logSampler{interval: interval}
}

func (s *logSampler) allow() (bool, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if now.Before(s.next) {
		s.suppressed++
		return false, 0
	}
	suppressed := s.suppressed
	s.suppressed = 0
	s.next = now.Add(s.interval)
	return true, suppressed
}
