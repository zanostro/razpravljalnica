package sub

import (
	"sync"

	pb "github.com/zanostro/razpravljalnica/gen/pb"
)

// Subscriber predstavlja eno odprto naročnino (en stream).
type Subscriber struct {
	userID int64
	topics map[int64]struct{}
	ch     chan *pb.MessageEvent
}

type Manager struct {
	mu   sync.RWMutex
	subs map[int64]*Subscriber // key: internal subscriber id
	next int64

	seqMu sync.Mutex
	seq   int64
}

func NewManager() *Manager {
	return &Manager{
		subs: make(map[int64]*Subscriber),
		next: 1,
		seq:  1,
	}
}

func (m *Manager) nextSeq() int64 {
	m.seqMu.Lock()
	defer m.seqMu.Unlock()
	s := m.seq
	m.seq++
	return s
}

func (m *Manager) Add(userID int64, topicIDs []int64) (subID int64, ch <-chan *pb.MessageEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()

	topics := make(map[int64]struct{}, len(topicIDs))
	for _, t := range topicIDs {
		topics[t] = struct{}{}
	}

	id := m.next
	m.next++

	s := &Subscriber{
		userID: userID,
		topics: topics,
		ch:     make(chan *pb.MessageEvent, 64), // buffer da publish ne blokira takoj
	}
	m.subs[id] = s
	return id, s.ch
}

func (m *Manager) Remove(subID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok := m.subs[subID]; ok {
		delete(m.subs, subID)
		close(s.ch)
	}
}

func (m *Manager) Publish(topicID int64, ev *pb.MessageEvent) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, s := range m.subs {
		if _, ok := s.topics[topicID]; !ok {
			continue
		}
		// non-blocking send; če je subscriber prepočasen, dropamo event
		select {
		case s.ch <- ev:
		default:
		}
	}
}

func (m *Manager) NextSeq() int64 {
	return m.nextSeq()
}
