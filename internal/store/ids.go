package store

func (s *Store) nextUser() int64 {
	id := s.nextUserID
	s.nextUserID++
	return id
}

func (s *Store) nextTopic() int64 {
	id := s.nextTopicID
	s.nextTopicID++
	return id
}

func (s *Store) nextMessage() int64 {
	id := s.nextMessageID
	s.nextMessageID++
	return id
}
