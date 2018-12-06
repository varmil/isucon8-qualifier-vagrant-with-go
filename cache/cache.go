package cache

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"

	. "torb/structs"

	"github.com/go-redis/redis"
	"github.com/json-iterator/go"
	funk "github.com/thoas/go-funk"
)

// MyRedisCli wraps original redis.Client
type MyRedisCli redis.Client

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// cache of non-canceled reservations
// { eventID: { reservationID: struct-ptr } }
var nonCanceledReservations = map[int64]map[int64]*Reservation{}

// mutex of non-canceled reservations map
var muOfNCR sync.RWMutex

/**
 * コネクションを張って、このパッケージのトップ変数に保持
 */
func CreateRedisClient() *MyRedisCli {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return (*MyRedisCli)(client)
}

// InitNonCanceledReservations makes map for eventIDs
func InitNonCanceledReservations(db *sql.DB) error {
	var reservations []*Reservation

	// fetch all
	{
		reservationsRows, err := db.Query("SELECT id, event_id, sheet_id, user_id, reserved_at FROM reservations WHERE canceled_at IS NULL")
		if err != nil {
			log.Fatal(err)
			return err
		}
		defer reservationsRows.Close()
		for reservationsRows.Next() {
			var reservation Reservation
			if err := reservationsRows.Scan(&reservation.ID, &reservation.EventID, &reservation.SheetID, &reservation.UserID, &reservation.ReservedAt); err != nil {
				log.Fatal(err)
				return err
			}
			reservation.ReservedAtUnix = reservation.ReservedAt.Unix()
			reservations = append(reservations, &reservation)
		}
	}

	// init map
	nonCanceledReservations = map[int64]map[int64]*Reservation{}

	// set cache
	eventIDs := funk.UniqInt64(funk.Map(reservations, func(x *Reservation) int64 {
		return x.EventID
	}).([]int64))
	for _, eid := range eventIDs {
		nonCanceledReservations[eid] = map[int64]*Reservation{}
	}
	for _, reservation := range reservations {
		HashSet(reservation.EventID, reservation.ID, reservation)
	}

	return nil
}

// GetReservations returns the non-canceled reservations for the eventID from cache
func GetReservations(eventID int64) []*Reservation {
	reservations := []*Reservation{}

	muOfNCR.RLock()
	reservationsMap, ok := nonCanceledReservations[eventID]
	muOfNCR.RUnlock()

	if ok {
		muOfNCR.RLock()
		reservations = funk.Values(reservationsMap).([]*Reservation)
		muOfNCR.RUnlock()

		sort.Slice(reservations, func(i, j int) bool { return reservations[i].ReservedAtUnix < reservations[j].ReservedAtUnix })
	}

	return reservations
}

// GetReservationsAll returns the non-canceled reservations for the multiple eventIDs from cache
func GetReservationsAll(eventIDs []int64) []*Reservation {
	var reservations []*Reservation

	// =========
	// bfTime := time.Now()
	// =========

	// NOTE: 見つからない場合 == 予約ゼロ
	// ここではすべてキャッシュに乗ってる前提なので、「空」は即ち予約ナシ。
	for _, eid := range eventIDs {
		deserialized := GetReservations(eid)
		if len(deserialized) > 0 {
			reservations = append(reservations, deserialized...)
		}
	}

	// =========
	// afTime := time.Now()
	// log.Printf("##### [GetReservationsAll] TIME: %f #####", afTime.Sub(bfTime).Seconds())
	// =========

	return reservations
}

// HashSet appends the reservation to cache
func HashSet(eventID int64, reservationID int64, reservation *Reservation) error {
	muOfNCR.Lock()
	defer muOfNCR.Unlock()

	if _, ok := nonCanceledReservations[eventID]; !ok {
		nonCanceledReservations[eventID] = map[int64]*Reservation{}
	}
	nonCanceledReservations[eventID][reservationID] = reservation
	return nil
}

// HashMSet appends the reservations to cache
func HashMSet(eventID int64, reservations []*Reservation) error {
	if len(reservations) == 0 {
		return nil
	}
	for _, reservation := range reservations {
		HashSet(reservation.EventID, reservation.ID, reservation)
	}
	return nil
}

// HashDelete deletes the key from cache
func HashDelete(eventID int64, reservationID int64) error {
	muOfNCR.Lock()
	defer muOfNCR.Unlock()
	delete(nonCanceledReservations[eventID], reservationID)
	return nil
}

func arrayToString(a []int64, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
}
