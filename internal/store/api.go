package store

import (
	"errors"
	"sort"
	"time"
)

var (
	ErrNotFound   = errors.New("not found")
	ErrForbidden  = errors.New("forbidden")
	ErrDuplicate  = errors.New("duplicate")
	ErrBadRequest = errors.New("bad request")
)

func (s *Store) CreateUser(username string) (User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if username == "" {
		return User{}, ErrBadRequest
	}

	id := s.nextUser()
	u := User{ID: id, Username: username}
	s.users[id] = u
	return u, nil
}

func (s *Store) CreateTopic(title string) (Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if title == "" {
		return Topic{}, ErrBadRequest
	}

	id := s.nextTopic()
	t := Topic{ID: id, Title: title}
	s.topics[id] = t
	return t, nil
}

func (s *Store) PostMessage(topicID, userID int64, text string) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if text == "" {
		return Message{}, ErrBadRequest
	}
	if _, ok := s.users[userID]; !ok {
		return Message{}, ErrNotFound
	}
	if _, ok := s.topics[topicID]; !ok {
		return Message{}, ErrNotFound
	}

	id := s.nextMessage()
	m := Message{
		ID:        id,
		TopicID:   topicID,
		UserID:    userID,
		Text:      text,
		CreatedAt: time.Now(),
		Likes:     0,
	}
	s.messages[MsgKey{TopicID: topicID, MessageID: id}] = &m
	return m, nil
}

func (s *Store) UpdateMessage(topicID, messageID, userID int64, text string) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if text == "" {
		return Message{}, ErrBadRequest
	}
	if _, ok := s.users[userID]; !ok {
		return Message{}, ErrNotFound
	}
	if _, ok := s.topics[topicID]; !ok {
		return Message{}, ErrNotFound
	}

	key := MsgKey{TopicID: topicID, MessageID: messageID}
	pm, ok := s.messages[key]
	if !ok {
		return Message{}, ErrNotFound
	}
	if pm.UserID != userID {
		return Message{}, ErrForbidden
	}

	pm.Text = text
	return *pm, nil
}

func (s *Store) DeleteMessage(topicID, messageID, userID int64) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.users[userID]; !ok {
		return Message{}, ErrNotFound
	}
	if _, ok := s.topics[topicID]; !ok {
		return Message{}, ErrNotFound
	}

	key := MsgKey{TopicID: topicID, MessageID: messageID}
	pm, ok := s.messages[key]
	if !ok {
		return Message{}, ErrNotFound
	}
	if pm.UserID != userID {
		return Message{}, ErrForbidden
	}

	// pobriši tudi likes
	for lk := range s.likes {
		if lk[0] == topicID && lk[1] == messageID {
			delete(s.likes, lk)
		}
	}

	deleted := *pm
	delete(s.messages, key)
	return deleted, nil
}

func (s *Store) LikeMessage(topicID, messageID, userID int64) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.users[userID]; !ok {
		return Message{}, ErrNotFound
	}
	if _, ok := s.topics[topicID]; !ok {
		return Message{}, ErrNotFound
	}

	key := MsgKey{TopicID: topicID, MessageID: messageID}
	pm, ok := s.messages[key]
	if !ok {
		return Message{}, ErrNotFound
	}

	// prepreči double-like
	lk := [3]int64{topicID, messageID, userID}
	if _, exists := s.likes[lk]; exists {
		return Message{}, ErrDuplicate
	}
	s.likes[lk] = struct{}{}

	pm.Likes++
	return *pm, nil
}

func (s *Store) ListTopics() ([]Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Topic, 0, len(s.topics))
	for _, t := range s.topics {
		out = append(out, t)
	}
	return out, nil
}

func (s *Store) GetMessages(topicID, fromMessageID int64, limit int32) ([]Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.topics[topicID]; !ok {
		return nil, ErrNotFound
	}
	if limit <= 0 {
		limit = 50
	}

	out := make([]Message, 0, limit)
	for k, pm := range s.messages {
		if k.TopicID == topicID && pm.ID >= fromMessageID {
			out = append(out, *pm)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	if int32(len(out)) > limit {
		out = out[:limit]
	}
	return out, nil
}
