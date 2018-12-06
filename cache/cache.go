package cache

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strings"

	. "torb/structs"

	"github.com/go-redis/redis"
	"github.com/json-iterator/go"
)

// MyRedisCli wraps original redis.Client
type MyRedisCli redis.Client

var json = jsoniter.ConfigCompatibleWithStandardLibrary

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
	nonCanceledReservations = map[int64]*SyncReservationMap{}

	// set cache（適当に十分大きな100くらいまで作っておく）
	const MaxEventLen = int64(100)
	for eid := int64(1); eid <= MaxEventLen; eid++ {
		nonCanceledReservations[eid] = NewSyncReservationMap()
	}
	for _, reservation := range reservations {
		HashSet(reservation.EventID, reservation.ID, reservation)
	}

	return nil
}

// GetReservations returns the non-canceled reservations for the eventID from cache
func GetReservations(eventID int64) []*Reservation {
	reservations := []*Reservation{}

	if syncMap, ok := nonCanceledReservations[eventID]; ok {
		reservations = syncMap.LoadAll()
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
	// 初期化時点で十分大きなMapを作っているはずなので、ここで新規作成はありえない
	if _, ok := nonCanceledReservations[eventID]; !ok {
		panic("nonCanceledReservations does not have enough eid")
	}
	nonCanceledReservations[eventID].Store(reservationID, reservation)
	return nil
}

// HashDelete deletes the key from cache
func HashDelete(eventID int64, reservationID int64) error {
	nonCanceledReservations[eventID].Delete(reservationID)
	return nil
}

func arrayToString(a []int64, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
}
