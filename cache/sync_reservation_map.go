package cache

import (
	"strconv"
	. "torb/structs"

	"github.com/orcaman/concurrent-map"
)

// SyncReservationMap contains cmap, the cmap has pointer of reservation as value
type SyncReservationMap struct {
	r cmap.ConcurrentMap
}

// NonCanceledReservations is the cache of non-canceled reservations
// { eventID: { reservationID: struct-ptr } }
var NonCanceledReservations = map[int64]*SyncReservationMap{}

// NewSyncReservationMap returns the instance
func NewSyncReservationMap() *SyncReservationMap {
	return &SyncReservationMap{r: cmap.New()}
}

// Store the instance
func (s *SyncReservationMap) Store(reservationID int64, value *Reservation) {
	s.r.Set(toString(reservationID), value)
}

// Load the instance, return nil if not exists
func (s *SyncReservationMap) Load(reservationID int64) *Reservation {
	t, ok := s.r.Get(toString(reservationID))
	if !ok {
		return nil
	}
	return t.(*Reservation)
}

// LoadAll the instances, return nil if not exists
func (s *SyncReservationMap) LoadAll() []*Reservation {
	var reservations []*Reservation

	for _, reservation := range s.r.Items() {
		reservations = append(reservations, reservation.(*Reservation))
	}
	return reservations
}

// Delete the instance
func (s *SyncReservationMap) Delete(reservationID int64) {
	s.r.Remove(toString(reservationID))
}

func toString(n int64) string {
	return strconv.FormatInt(n, 10)
}
