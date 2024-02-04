package main

import (
	"sync"
)

type Agent struct {
	m      sync.Mutex
	subs   map[string][]chan entry
	closed bool
	quit   chan struct{}
}

func NewAgent() *Agent {
	return &Agent{subs: make(map[string][]chan entry), quit: make(chan struct{})}
}

func (a *Agent) SubscribeOnce(key string) <-chan entry {
	a.m.Lock()
	defer a.m.Unlock()
	if a.closed {
		return nil
	}
	ch := make(chan entry, 1)
	a.subs[key] = append(a.subs[key], ch)
	return ch
}

func (a *Agent) Publish(key string, ent entry) {
	a.m.Lock()
	defer a.m.Unlock()
	if a.closed {
		return
	}

	for _, ch := range a.subs[key] {
		ch <- ent
		close(ch)
	}
}

func (a *Agent) Close() {
	a.m.Lock()
	defer a.m.Unlock()
	if a.closed {
		return
	}
	a.closed = true
	close(a.quit)
}
