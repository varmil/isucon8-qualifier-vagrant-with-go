package cache

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

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
var muOfNCR sync.Mutex

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
	eventIDs := funk.UniqInt64(funk.Map(reservations, func(x *Reservation) int64 {
		return x.EventID
	}).([]int64))
	for _, eid := range eventIDs {
		nonCanceledReservations[eid] = map[int64]*Reservation{}
	}

	// set cache
	for _, reservation := range reservations {
		nonCanceledReservations[reservation.EventID][reservation.ID] = reservation
	}

	return nil
}

/**
 * HGetAll。結果がなければempty slice
 */
func (cli *MyRedisCli) GetReservations(eventID int64) ([]*Reservation, error) {
	var reservations []*Reservation

	muOfNCR.Lock()
	defer muOfNCR.Unlock()
	if reservationsMap, ok := nonCanceledReservations[eventID]; ok {
		for _, r := range reservationsMap {
			reservations = append(reservations, r)
		}
		return reservations, nil
	}
	return []*Reservation{}, nil

	// var reservations []*Reservation
	// var val map[string]string
	// var err error

	// key := "reservations.notCanceled.eid." + strconv.FormatInt(eventID, 10)
	// val, err = cli.HGetAll(key).Result()

	// if err != redis.Nil {
	// 	for _, reservationStr := range funk.Values(val).([]string) {
	// 		var deserialized *Reservation
	// 		err = json.Unmarshal([]byte(reservationStr), &deserialized)
	// 		if err != nil {
	// 			log.Fatal(err)
	// 			return nil, err
	// 		}
	// 		if deserialized.ID != 0 {
	// 			// log.Printf("HGetAll: eid: %v, len: %v", eventID, deserialized)
	// 			reservations = append(reservations, deserialized)
	// 		}
	// 	}
	// }
	// return reservations, nil
}

func (cli *MyRedisCli) HashSet(eventID int64, reservationID int64, reservation *Reservation) error {
	muOfNCR.Lock()
	defer muOfNCR.Unlock()

	if _, ok := nonCanceledReservations[eventID]; !ok {
		nonCanceledReservations[eventID] = map[int64]*Reservation{}
	}
	nonCanceledReservations[eventID][reservationID] = reservation
	return nil

	// key := "reservations.notCanceled.eid." + strconv.FormatInt(eventID, 10)
	// bytes, err := json.Marshal(reservation)
	// if err != nil {
	// 	panic("MAP ERROR")
	// }
	// cli.HSet(key, strconv.FormatInt(reservationID, 10), bytes)
	// return nil
}

func (cli *MyRedisCli) HashMSet(eventID int64, reservations []*Reservation) error {
	if len(reservations) == 0 {
		return nil
	}
	for _, reservation := range reservations {
		cli.HashSet(reservation.EventID, reservation.ID, reservation)
	}
	return nil

	// // map []*Reservation to map[string]*Reservation{}
	// reservationsMap := funk.Map(reservations, func(x *Reservation) (string, interface{}) {
	// 	bytes, err := json.Marshal(x)
	// 	if err != nil {
	// 		panic("MAP ERROR")
	// 	}
	// 	return strconv.FormatInt(x.ID, 10), bytes
	// }).(map[string]interface{})

	// // log.Printf("HMSET: %v, len: %v", eventID, len(reservations))
	// cli.HMSet("reservations.notCanceled.eid."+strconv.FormatInt(eventID, 10), reservationsMap)
	// return nil
}

func (cli *MyRedisCli) HashDelete(eventID int64, reservationID int64) error {
	muOfNCR.Lock()
	defer muOfNCR.Unlock()
	delete(nonCanceledReservations[eventID], reservationID)
	// cli.HDel("reservations.notCanceled.eid."+strconv.FormatInt(eventID, 10), strconv.FormatInt(reservationID, 10))
	return nil
}

/**
 * MySQLとRedis併用
 */
func (cli *MyRedisCli) FetchAndCacheReservations(db *sql.DB, eventIDs []int64) ([]*Reservation, error) {
	var reservations []*Reservation

	// search the cache for each item
	// NOTE: 見つからない場合 == 予約ゼロ
	// ここではすべてキャッシュに乗ってる前提なので、「空」は即ち予約ナシ。
	{
		// =========
		bfTime := time.Now()
		// =========

		for _, eid := range eventIDs {
			deserialized, err := cli.GetReservations(eid)
			if err != nil {
				log.Fatal(err)
				return nil, err
			}
			if len(deserialized) > 0 {
				reservations = append(reservations, deserialized...)
			}
		}

		// =========
		afTime := time.Now()
		log.Printf("##### [FetchAndCacheReservations] TIME: %f #####", afTime.Sub(bfTime).Seconds())
		// =========
	}

	sort.Slice(reservations, func(i, j int) bool { return reservations[i].ReservedAtUnix < reservations[j].ReservedAtUnix })
	return reservations, nil
}

func arrayToString(a []int64, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
}
