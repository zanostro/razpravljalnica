package store

import (
	"sync"
	"time"
)

// Core modeli (notranji, ne pb tipi)
type User struct {
	ID       int64
	Username string
}

type Topic struct {
	ID    int64
	Title string
}

type Message struct {
	ID        int64
	TopicID   int64
	UserID    int64
	Text      string
	CreatedAt time.Time
	Likes     int32
}

// Key za hitro mapiranje message-ov po (topic,message)
type MsgKey struct {
	TopicID   int64
	MessageID int64
}

type Store struct {
	mu sync.RWMutex

	// podatki
	users    map[int64]User
	topics   map[int64]Topic
	messages map[MsgKey]*Message

	// likes za preprečevanje dvojnega like-a
	likes map[[3]int64]struct{} // [topic_id, message_id, user_id]

	// ID generatorji
	nextUserID    int64
	nextTopicID   int64
	nextMessageID int64

	// Sequence number for chain replication
	nextSequenceNumber int64
}

func New() *Store {
	return &Store{
		users:              make(map[int64]User),
		topics:             make(map[int64]Topic),
		messages:           make(map[MsgKey]*Message),
		likes:              make(map[[3]int64]struct{}),
		nextUserID:         1,
		nextTopicID:        1,
		nextMessageID:      1,
		nextSequenceNumber: 1,
	}
}

// GetNextSequenceNumber returns a monotonically increasing sequence number
func (s *Store) GetNextSequenceNumber() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := s.nextSequenceNumber
	s.nextSequenceNumber++
	return seq
}

// GetLastSequenceNumber returns the last assigned sequence number
func (s *Store) GetLastSequenceNumber() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextSequenceNumber - 1
}
