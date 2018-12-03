package cache

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	. "torb/structs"

	"github.com/go-redis/redis"
	funk "github.com/thoas/go-funk"
)

var redisCli *redis.Client

/**
 * コネクションを張って、このパッケージのトップ変数に保持
 */
func CreateRedisClient() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// set the connection
	redisCli = client
	return client
}

/**
 * HGetAll。結果がなければempty slice
 */
func GetReservations(eventID int64) ([]*Reservation, error) {
	var reservations []*Reservation
	var val map[string]string
	var err error

	key := "reservations.notCanceled.eid." + strconv.FormatInt(eventID, 10)
	val, err = redisCli.HGetAll(key).Result()

	if err != redis.Nil {
		for _, reservationStr := range funk.Values(val).([]string) {
			var deserialized *Reservation
			err = json.Unmarshal([]byte(reservationStr), &deserialized)
			if err != nil {
				log.Fatal(err)
				return nil, err
			}
			if deserialized.ID != 0 {
				// log.Printf("HGetAll: eid: %v, len: %v", eventID, deserialized)
				reservations = append(reservations, deserialized)
			}
		}
	}
	return reservations, nil
}

/**
 * SMEMBERS。結果がなければempty slice
 */
// func GetCanceledReservations() ([]*Reservation, error) {
// 	var reservations []*Reservation

// 	key := "reservations.canceled"
// 	val, err := redisCli.SMembers(key).Result()

// 	if err != redis.Nil {
// 		for _, reservationStr := range val {
// 			var deserialized *Reservation
// 			err = json.Unmarshal([]byte(reservationStr), &deserialized)
// 			if err != nil {
// 				log.Fatal(err)
// 				return nil, err
// 			}
// 			if deserialized.ID != 0 {
// 				reservations = append(reservations, deserialized)
// 			}
// 		}
// 	}
// 	return reservations, nil
// }

// SADD （REPORT用のキャッシュ）
// func SAddCanceledReservations(reservations []Reservation) error {
// 	if len(reservations) == 0 {
// 		return nil
// 	}

// 	var result []interface{}
// 	for _, reservation := range reservations {
// 		bytes, err := json.Marshal(reservation)
// 		if err != nil {
// 			panic("MAP ERROR")
// 		}
// 		result = append(result, bytes)
// 	}

// 	redisCli.SAdd("reservations.canceled", result...)
// 	return nil
// }

/**
 * HMSET (cause error when use pipe.HMSet)
 */
func HMSet(eventID int64, reservations []*Reservation, hmset func(key string, fields map[string]interface{}) *redis.StatusCmd) error {
	if len(reservations) == 0 {
		// log.Print("reservations is empty, so do nothing")
		return nil
	}

	// map []*Reservation to map[string]*Reservation{}
	reservationsMap := funk.Map(reservations, func(x *Reservation) (string, interface{}) {
		bytes, err := json.Marshal(x)
		if err != nil {
			panic("MAP ERROR")
		}
		return strconv.FormatInt(x.ID, 10), bytes
	}).(map[string]interface{})

	// log.Printf("HMSET: %v, len: %v", eventID, len(reservations))
	hmset("reservations.notCanceled.eid."+strconv.FormatInt(eventID, 10), reservationsMap)
	return nil
}

/**
 * HSET
 */
func HSet(eventID int64, reservationID int64, reservation *Reservation) error {
	key := "reservations.notCanceled.eid." + strconv.FormatInt(eventID, 10)
	bytes, err := json.Marshal(reservation)
	if err != nil {
		panic("MAP ERROR")
	}
	redisCli.HSet(key, strconv.FormatInt(reservationID, 10), bytes)
	return nil
}

/**
 * HDEL
 */
func HDel(eventID int64, reservationID int64) error {
	redisCli.HDel("reservations.notCanceled.eid."+strconv.FormatInt(eventID, 10), strconv.FormatInt(reservationID, 10))
	return nil
}

/**
 * MySQLとRedis併用
 */
func FetchAndCacheReservations(db *sql.DB, eventIDs []int64) ([]*Reservation, error) {
	var reservations []*Reservation
	var eventIDsForSearching []int64

	// search the cache for each item
	// NOTE: 見つからない場合 == キャッシュがない or 予約ゼロ。後者の場合を区別する
	{
		for _, eid := range eventIDs {
			deserialized, err := GetReservations(eid)
			if err != nil {
				log.Fatal(err)
				return nil, err
			}
			if len(deserialized) > 0 {
				// 見つかったら結果配列にconcatして、eventIDsから当該IDを削除する（＝検索対象から除外する）
				reservations = append(reservations, deserialized...)
			} else {
				// 見つからなければ、検索用IDに追加
				eventIDsForSearching = append(eventIDsForSearching, eid)
			}
		}
	}

	// SELECT query (slow!)
	if len(eventIDsForSearching) > 0 {
		sql := fmt.Sprintf("SELECT id, event_id, sheet_id, user_id, reserved_at FROM reservations WHERE event_id IN (%s) AND canceled_at IS NULL", arrayToString(eventIDsForSearching, ","))
		reservationsRows, err := db.Query(sql)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
		defer reservationsRows.Close()
		for reservationsRows.Next() {
			var reservation Reservation
			if err := reservationsRows.Scan(&reservation.ID, &reservation.EventID, &reservation.SheetID, &reservation.UserID, &reservation.ReservedAt); err != nil {
				log.Fatal(err)
				return nil, err
			}
			reservation.ReservedAtUnix = reservation.ReservedAt.Unix()
			reservations = append(reservations, &reservation)
		}

		// set each item in the cache
		{
			for _, eid := range eventIDsForSearching {
				r := funk.Filter(reservations, func(x *Reservation) bool {
					return x.EventID == eid
				})
				if r != nil {
					reservationsForEventID := r.([]*Reservation)
					HMSet(eid, reservationsForEventID, redisCli.HMSet)
				}
			}
		}
	}

	sort.Slice(reservations, func(i, j int) bool { return reservations[i].ReservedAtUnix < reservations[j].ReservedAtUnix })
	return reservations, nil
}

func arrayToString(a []int64, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
}
