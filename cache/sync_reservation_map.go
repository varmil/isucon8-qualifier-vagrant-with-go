package cache

import (
	"sync"
	. "torb/structs"
)

// SyncReservationMap contains pointer of reservation with lock
type SyncReservationMap struct {
	mu          sync.RWMutex
	reservation map[int64]*Reservation
}

// cache of non-canceled reservations
// { eventID: { reservationID: struct-ptr } }
var nonCanceledReservations = map[int64]*SyncReservationMap{}

// NewSyncReservationMap returns the instance
func NewSyncReservationMap() *SyncReservationMap {
	return &SyncReservationMap{reservation: map[int64]*Reservation{}}
}

// Store the instance
func (s *SyncReservationMap) Store(reservationID int64, value *Reservation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reservation[reservationID] = value
}

// Load the instance, return nil if not exists
func (s *SyncReservationMap) Load(reservationID int64) *Reservation {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.reservation[reservationID]
	if !ok {
		return nil
	}
	return t
}

// LoadAll the instances, return nil if not exists
func (s *SyncReservationMap) LoadAll() []*Reservation {
	var reservations []*Reservation
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, reservation := range s.reservation {
		reservations = append(reservations, reservation)
	}
	return reservations
}

// Delete the instance
func (s *SyncReservationMap) Delete(reservationID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.reservation, reservationID)
}
