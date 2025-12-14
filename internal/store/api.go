package store

import "errors"

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

func (s *Store) PostMessage(topicID, userID int64, content string) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// TODO implement
	return Message{}, nil
}

func (s *Store) UpdateMessage(topicID, messageID, userID int64, content string) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// TODO implement
	return Message{}, nil
}

func (s *Store) DeleteMessage(topicID, messageID, userID int64) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// TODO implement
	return Message{}, nil
}

func (s *Store) LikeMessage(topicID, messageID, userID int64) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// TODO implement
	return Message{}, nil
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

func (s *Store) GetMessages(topicID, fromMessageID int64) ([]Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// TODO implement
	return nil, nil
}
