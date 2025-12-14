package store

import "sync"

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
	ID      int64
	TopicID int64
	UserID  int64
	Content string
	Likes   int64
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
}

func New() *Store {
	return &Store{
		users:          make(map[int64]User),
		topics:         make(map[int64]Topic),
		messages:       make(map[MsgKey]*Message),
		likes:          make(map[[3]int64]struct{}),
		nextUserID:     1,
		nextTopicID:    1,
		nextMessageID:  1,
	}
}
