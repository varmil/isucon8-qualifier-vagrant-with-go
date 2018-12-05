package cache

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	. "torb/structs"

	"github.com/go-redis/redis"
	"github.com/json-iterator/go"
	funk "github.com/thoas/go-funk"
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

/**
 * HGetAll。結果がなければempty slice
 */
func (cli *MyRedisCli) GetReservations(eventID int64) ([]*Reservation, error) {
	var reservations []*Reservation
	var val map[string]string
	var err error

	key := "reservations.notCanceled.eid." + strconv.FormatInt(eventID, 10)
	val, err = cli.HGetAll(key).Result()

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
 * HSET
 */
func (cli *MyRedisCli) HashSet(eventID int64, reservationID int64, reservation *Reservation) error {
	key := "reservations.notCanceled.eid." + strconv.FormatInt(eventID, 10)
	bytes, err := json.Marshal(reservation)
	if err != nil {
		panic("MAP ERROR")
	}
	cli.HSet(key, strconv.FormatInt(reservationID, 10), bytes)
	return nil
}

func (cli *MyRedisCli) HashMSet(eventID int64, reservations []*Reservation) error {
	if len(reservations) == 0 {
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
	cli.HMSet("reservations.notCanceled.eid."+strconv.FormatInt(eventID, 10), reservationsMap)
	return nil
}

/**
 * HDEL
 */
func (cli *MyRedisCli) HashDelete(eventID int64, reservationID int64) error {
	cli.HDel("reservations.notCanceled.eid."+strconv.FormatInt(eventID, 10), strconv.FormatInt(reservationID, 10))
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
				// 見つかったら結果配列にconcat
				reservations = append(reservations, deserialized...)
			} else {
				// do nothing
			}
		}

		// =========
		afTime := time.Now()
		log.Printf("##### HMGET TIME: %f #####", afTime.Sub(bfTime).Seconds())
		// =========
	}

	sort.Slice(reservations, func(i, j int) bool { return reservations[i].ReservedAtUnix < reservations[j].ReservedAtUnix })
	return reservations, nil
}

func arrayToString(a []int64, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
}
