/**
 * ## REQUIREMENT
 * go get -u github.com/oxequa/realize
 * go get github.com/joho/godotenv
 * go get github.com/thoas/go-funk
 * go get github.com/patrickmn/go-cache
 * go get -u github.com/go-redis/redis
 * go get github.com/json-iterator/go
 *
 *
 * ## HOW TO SERVE
 * sudo -i -u isucon
 * cd /home/isucon/torb/webapp/go
 * GOPATH=`pwd`:`pwd`/vendor:/home/isucon/go realize s --no-config --path="./src/torb" --run
 *
 *
 * ### SCORE LOG
 *   800  : SELECT FOR UPDATE が無駄に見えたので削除。
 *  4810  : getEvent() の reservations テーブルSELECTに関して N+1を解決。reservations.user_id に INDEX追加
 *  7700  : go-cacheを用いてsheetsをslice格納してキャッシュ。この時点でgetEvent()はもはやボトルネックではないが、まだ /api/events/:id/actions/reserve 自体は遅い。
 *  9510  : getEvents()が内部で大量にgetEvent()をcallしていたので、呼出回数を実質１回にした。 /api/users/:id が次のボトルネックっぽい。
 * 26078  : WIP getEventsInをcacheしたがrace conditionで弾かれるので、ロックを入れないと駄目かも
 * 19292  : WIP ↑引き続き。Cache更新タイミングをトランザクションのcommit後に変更することで若干緩和。/actions/reservations のFOR UPDATEを削除
 * 9k-29k : WIP ↑引き続き。 fetchAndCacheReservations() に順次置き換え
 * 37k    : WIP ↑引き続き。 マニュアルをもう一度読む。/admin/api/reports/sales が原因で負荷レベルが上がらない。ORDER BYせずとも何とPASSした。罠。
 * refactor goCache --> redis
 * refactor WATCH（SETNX）を使うか、HASH型にしてATOMICに書き換えできるようにする。
 */
package main

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/sessions"
	"github.com/joho/godotenv"
	"github.com/json-iterator/go"
	"github.com/labstack/echo"
	"github.com/labstack/echo-contrib/session"
	"github.com/labstack/echo/middleware"
	"github.com/patrickmn/go-cache"
	funk "github.com/thoas/go-funk"

	sess "torb/session"
)

type User struct {
	ID        int64  `json:"id,omitempty"`
	Nickname  string `json:"nickname,omitempty"`
	LoginName string `json:"login_name,omitempty"`
	PassHash  string `json:"pass_hash,omitempty"`
}

type Event struct {
	ID       int64  `json:"id,omitempty"`
	Title    string `json:"title,omitempty"`
	PublicFg bool   `json:"public,omitempty"`
	ClosedFg bool   `json:"closed,omitempty"`
	Price    int64  `json:"price,omitempty"`

	Total   int                `json:"total"`
	Remains int                `json:"remains"`
	Sheets  map[string]*Sheets `json:"sheets,omitempty"`
}

type Sheets struct {
	Total   int      `json:"total"`
	Remains int      `json:"remains"`
	Detail  []*Sheet `json:"detail,omitempty"`
	Price   int64    `json:"price"`
}

type Sheet struct {
	ID    int64  `json:"-"`
	Rank  string `json:"-"`
	Num   int64  `json:"num"`
	Price int64  `json:"-"`

	Mine           bool       `json:"mine,omitempty"`
	Reserved       bool       `json:"reserved,omitempty"`
	ReservedAt     *time.Time `json:"-"`
	ReservedAtUnix int64      `json:"reserved_at,omitempty"`
}

type Reservation struct {
	ID         int64      `json:"id"`
	EventID    int64      `json:"event_id,omitempty"`
	SheetID    int64      `json:"sheet_id,omitempty"`
	UserID     int64      `json:"user_id,omitempty"`
	ReservedAt *time.Time `json:"-"`
	CanceledAt *time.Time `json:"-"`

	Event          *Event `json:"event,omitempty"`
	SheetRank      string `json:"sheet_rank,omitempty"`
	SheetNum       int64  `json:"sheet_num,omitempty"`
	Price          int64  `json:"price,omitempty"`
	ReservedAtUnix int64  `json:"reserved_at,omitempty"`
	CanceledAtUnix int64  `json:"canceled_at,omitempty"`
}

type Administrator struct {
	ID        int64  `json:"id,omitempty"`
	Nickname  string `json:"nickname,omitempty"`
	LoginName string `json:"login_name,omitempty"`
	PassHash  string `json:"pass_hash,omitempty"`
}

func createRedisClient() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	return client
}

func arrayToString(a []int64, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
	//return strings.Trim(strings.Join(strings.Split(fmt.Sprint(a), " "), delim), "[]")
	//return strings.Trim(strings.Join(strings.Fields(fmt.Sprint(a)), delim), "[]")
}

func cacheSheets() {
	var sheets []Sheet
	sheetsRows, err := db.Query("SELECT * FROM sheets ORDER BY `rank`, num")
	if err != nil {
		log.Fatal(err)
	}
	defer sheetsRows.Close()

	for sheetsRows.Next() {
		var sheet Sheet
		if err := sheetsRows.Scan(&sheet.ID, &sheet.Rank, &sheet.Num, &sheet.Price); err != nil {
			log.Fatal(err)
		}
		sheets = append(sheets, sheet)
	}
	goCache.Set("sheetsSlice", sheets, cache.DefaultExpiration)
}

func loginRequired(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if _, err := getLoginUser(c); err != nil {
			return resError(c, "login_required", 401)
		}
		return next(c)
	}
}

func adminLoginRequired(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if _, err := getLoginAdministrator(c); err != nil {
			return resError(c, "admin_login_required", 401)
		}
		return next(c)
	}
}

func getLoginUser(c echo.Context) (*User, error) {
	userID := sess.SessUserID(c)
	if userID == 0 {
		return nil, errors.New("not logged in")
	}
	var user User
	err := db.QueryRow("SELECT id, nickname FROM users WHERE id = ?", userID).Scan(&user.ID, &user.Nickname)
	return &user, err
}

func getLoginAdministrator(c echo.Context) (*Administrator, error) {
	administratorID := sess.SessAdministratorID(c)
	if administratorID == 0 {
		return nil, errors.New("not logged in")
	}
	var administrator Administrator
	err := db.QueryRow("SELECT id, nickname FROM administrators WHERE id = ?", administratorID).Scan(&administrator.ID, &administrator.Nickname)
	return &administrator, err
}

func getEvents(all bool) ([]*Event, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	rows, err := tx.Query("SELECT * FROM events ORDER BY id ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*Event
	for rows.Next() {
		var event Event
		if err := rows.Scan(&event.ID, &event.Title, &event.PublicFg, &event.ClosedFg, &event.Price); err != nil {
			return nil, err
		}
		if !all && !event.PublicFg {
			continue
		}
		events = append(events, &event)
	}

	bfTime := time.Now()

	ids := funk.Map(events, func(x *Event) int64 {
		return x.ID
	})
	addedEvents, err := getEventsIn(ids.([]int64), -1)
	for i := range addedEvents {
		for k := range addedEvents[i].Sheets {
			addedEvents[i].Sheets[k].Detail = nil
		}
		events[i] = addedEvents[i]
	}

	afTime := time.Now()
	log.Printf("[getEvents] for loop with getEvent() TIME: %f", afTime.Sub(bfTime).Seconds())

	return events, nil
}

func getEventsIn(eventIDs []int64, loginUserID int64) ([]*Event, error) {
	var events []*Event
	inClause := arrayToString(eventIDs, ",")
	// log.Print(inClause)

	// EVENTS
	{
		sql := fmt.Sprintf("SELECT * FROM events WHERE id IN (%s)", inClause)
		eventRows, err := db.Query(sql)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
		defer eventRows.Close()
		for eventRows.Next() {
			var event Event
			if err := eventRows.Scan(&event.ID, &event.Title, &event.PublicFg, &event.ClosedFg, &event.Price); err != nil {
				log.Fatal(err)
				return nil, err
			}

			event.Sheets = map[string]*Sheets{
				"S": &Sheets{},
				"A": &Sheets{},
				"B": &Sheets{},
				"C": &Sheets{},
			}
			events = append(events, &event)
		}
	}

	// RESERVATIONS
	var reservations []*Reservation
	{
		// =========
		bfTime := time.Now()
		// =========

		var err error
		reservations, err = fetchAndCacheReservations(eventIDs, nil)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		// =========
		afTime := time.Now()
		log.Printf("##### [getEventsIn] RESERVATIONS TIME: %f #####", afTime.Sub(bfTime).Seconds())
		// =========
	}

	// ADD INFO
	for i := range events {
		err := addEventInfo(events[i], reservations, loginUserID)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
	}

	return events, nil
}

func getEvent(eventID, loginUserID int64) (*Event, error) {
	var event Event
	if err := db.QueryRow("SELECT * FROM events WHERE id = ?", eventID).Scan(&event.ID, &event.Title, &event.PublicFg, &event.ClosedFg, &event.Price); err != nil {
		return nil, err
	}
	event.Sheets = map[string]*Sheets{
		"S": &Sheets{},
		"A": &Sheets{},
		"B": &Sheets{},
		"C": &Sheets{},
	}

	// ----- まず reservations 全部取得。その後APサーバで処理 -----
	var reservations []*Reservation
	var err error
	{
		reservations, err = fetchAndCacheReservations([]int64{eventID}, nil)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
	}
	// ---------------------------------------

	// ----- シートを走査 ----------------------
	err = addEventInfo(&event, reservations, loginUserID)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	// ---------------------------------------

	return &event, nil
}

/**
 * HGetAll。結果がなければempty slice
 */
func getReservationsFromCache(eventID int64, pipe redis.Pipeliner) ([]*Reservation, error) {
	var reservations []*Reservation
	var val map[string]string
	var err error

	key := "reservations.notCanceled.eid." + strconv.FormatInt(eventID, 10)
	if pipe == nil {
		val, err = redisCli.HGetAll(key).Result()
	} else {
		val, err = pipe.HGetAll(key).Result()
	}

	if err != redis.Nil {
		for _, reservationStr := range funk.Values(val).([]string) {
			var deserialized *Reservation
			err = json.Unmarshal([]byte(reservationStr), &deserialized)
			if err != nil {
				log.Fatal(err)
				return nil, err
			}
			if deserialized.ID != 0 {
				// log.Printf("getFromCache: eid: %v, len: %v", eventID, deserialized)
				reservations = append(reservations, deserialized)
			}
		}
	}
	return reservations, nil
}

/**
 * HMSET (cause error when use pipe.HMSet)
 */
func setToCache(eventID int64, reservations []*Reservation, hmset func(key string, fields map[string]interface{}) *redis.StatusCmd) error {
	// map []*Reservation to map[string]*Reservation{}
	reservationsMap := funk.Map(reservations, func(x *Reservation) (string, interface{}) {
		bytes, err := json.Marshal(x)
		if err != nil {
			panic("MAP ERROR")
		}
		return strconv.FormatInt(x.ID, 10), bytes
	}).(map[string]interface{})

	// log.Printf("CNOT HIT: %v, len: %v", eventID, len(reservations))
	hmset("reservations.notCanceled.eid."+strconv.FormatInt(eventID, 10), reservationsMap)
	return nil
}

/**
 * HSET
 */
func hset(eventID int64, reservationID int64, reservation *Reservation) error {
	bytes, err := json.Marshal(reservation)
	if err != nil {
		panic("MAP ERROR")
	}
	redisCli.HSet("reservations.notCanceled.eid."+strconv.FormatInt(eventID, 10), strconv.FormatInt(reservationID, 10), bytes)
	return nil
}

/**
 * HDEL
 */
func hdel(eventID int64, reservationID int64) error {
	redisCli.HDel("reservations.notCanceled.eid."+strconv.FormatInt(eventID, 10), strconv.FormatInt(reservationID, 10))
	return nil
}

func fetchAndCacheReservations(eventIDs []int64, pipe redis.Pipeliner) ([]*Reservation, error) {
	var reservations []*Reservation
	var eventIDsForSearching []int64

	// search the cache for each item
	// TODO: 見つからない場合 == キャッシュがない or 予約ゼロ。後者の場合を区別する
	{
		for _, eid := range eventIDs {
			deserialized, err := getReservationsFromCache(eid, pipe)
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
					setToCache(eid, reservationsForEventID, redisCli.HMSet)
				}
			}
		}
	}

	sort.Slice(reservations, func(i, j int) bool { return reservations[i].ReservedAtUnix < reservations[j].ReservedAtUnix })

	return reservations, nil
}

func addEventInfo(event *Event, reservations []*Reservation, loginUserID int64) error {
	// ----- sheets テーブルは不変なのでキャッシュしている -----
	var sheets []Sheet
	if x, found := goCache.Get("sheetsSlice"); found {
		sheets = x.([]Sheet)
	} else {
		log.Fatal("SHEETS CACHE NOT FOUND")
	}

	for _, sheet := range sheets {
		sheetCopy := Sheet{
			ID:    sheet.ID,
			Rank:  sheet.Rank,
			Num:   sheet.Num,
			Price: sheet.Price,
		}

		event.Sheets[sheetCopy.Rank].Price = event.Price + sheetCopy.Price
		event.Total++
		event.Sheets[sheetCopy.Rank].Total++

		// sheet_idでfind
		var reservation Reservation
		for _, v := range reservations {
			if v.SheetID == sheetCopy.ID && event.ID == v.EventID {
				reservation = *v
				break
			}
		}

		if (Reservation{}) == reservation {
			// そのシートに予約が入っていない場合
			event.Remains++
			event.Sheets[sheetCopy.Rank].Remains++
		} else {
			// シートに予約が入っている場合、最もReservedAtが早いものを取得
			sheetCopy.Mine = reservation.UserID == loginUserID
			sheetCopy.Reserved = true
			sheetCopy.ReservedAtUnix = reservation.ReservedAtUnix
		}

		event.Sheets[sheetCopy.Rank].Detail = append(event.Sheets[sheetCopy.Rank].Detail, &sheetCopy)
	}

	return nil
}

/**
 * INSERT INTO reservations
 */
func tryInsertReservation(user User, event Event, rank string) (int64, Sheet, error) {
	var reservationID int64
	var sheet Sheet
	key := "reservations.notCanceled.eid." + strconv.FormatInt(event.ID, 10)

	err := redisCli.Watch(func(tx *redis.Tx) error {
		// fetch reserved sheetIDs of the event
		pipe := tx.TxPipeline()
		reservations, err := fetchAndCacheReservations([]int64{event.ID}, nil)
		if err != nil {
			return err
		}
		sheetIDs := funk.Map(reservations, func(x *Reservation) int64 {
			return x.SheetID
		}).([]int64)
		if len(sheetIDs) == 0 {
			sheetIDs = append(sheetIDs, 0)
		}

		// fetch a NOT reserved sheet
		query := fmt.Sprintf("SELECT * FROM sheets WHERE id NOT IN (%s) AND `rank` = '%s' ORDER BY RAND() LIMIT 1", arrayToString(sheetIDs, ","), rank)
		if err := db.QueryRow(query).Scan(&sheet.ID, &sheet.Rank, &sheet.Num, &sheet.Price); err != nil {
			if err == sql.ErrNoRows {
				return sql.ErrNoRows
			}
			return err
		}

		dbtx, err := db.Begin()
		if err != nil {
			return err
		}
		res, err := dbtx.Exec("INSERT INTO reservations (event_id, sheet_id, user_id, reserved_at) VALUES (?, ?, ?, ?)", event.ID, sheet.ID, user.ID, time.Now().UTC().Format("2006-01-02 15:04:05.000000"))
		if err != nil {
			dbtx.Rollback()
			return err
		}
		reservationID, err = res.LastInsertId()
		if err != nil {
			dbtx.Rollback()
			return err
		}

		// insert the reservation into cache before commit DB
		{
			time := time.Now().UTC()
			reservation := Reservation{ID: reservationID, EventID: event.ID, SheetID: sheet.ID, UserID: user.ID, ReservedAt: &time, ReservedAtUnix: time.Unix()}
			hset(event.ID, reservationID, &reservation)

			_, err = pipe.Exec()
			if err != nil {
				dbtx.Rollback()
				return err
			}
		}

		if err := dbtx.Commit(); err != nil {
			dbtx.Rollback()
			return err
		}

		return err
	}, key)

	return reservationID, sheet, err
}

func sanitizeEvent(e *Event) *Event {
	sanitized := *e
	sanitized.Price = 0
	sanitized.PublicFg = false
	sanitized.ClosedFg = false
	return &sanitized
}

func fillinUser(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if user, err := getLoginUser(c); err == nil {
			c.Set("user", user)
		}
		return next(c)
	}
}

func fillinAdministrator(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if administrator, err := getLoginAdministrator(c); err == nil {
			c.Set("administrator", administrator)
		}
		return next(c)
	}
}

func validateRank(rank string) bool {
	var count int
	db.QueryRow("SELECT COUNT(*) FROM sheets WHERE `rank` = ?", rank).Scan(&count)
	return count > 0
}

type Renderer struct {
	templates *template.Template
}

func (r *Renderer) Render(w io.Writer, name string, data interface{}, c echo.Context) error {
	return r.templates.ExecuteTemplate(w, name, data)
}

var db *sql.DB
var goCache *cache.Cache
var mx *sync.Mutex
var redisCli *redis.Client
var json = jsoniter.ConfigCompatibleWithStandardLibrary

func main() {
	var err error
	// log.SetFlags(log.Lshortfile)

	{
		err := godotenv.Load("../env.sh")
		if err != nil {
			log.Fatal("Error loading .env file")
		}
	}

	{
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4",
			os.Getenv("DB_USER"), os.Getenv("DB_PASS"),
			os.Getenv("DB_HOST"), os.Getenv("DB_PORT"),
			os.Getenv("DB_DATABASE"),
		)
		// log.Printf("DSN IS %s", dsn)
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			log.Fatal(err)
		}
	}

	// redis
	{
		redisCli = createRedisClient()
		redisCli.FlushAll()
	}

	// go-cache
	{
		goCache = cache.New(60*time.Minute, 120*time.Minute)
		cacheSheets()
	}

	// mutex
	mx = new(sync.Mutex)

	e := echo.New()
	funcs := template.FuncMap{
		"encode_json": func(v interface{}) string {
			b, _ := json.Marshal(v)
			return string(b)
		},
	}
	e.Renderer = &Renderer{
		templates: template.Must(template.New("").Delims("[[", "]]").Funcs(funcs).ParseGlob("views/*.tmpl")),
	}
	e.Use(session.Middleware(sessions.NewCookieStore([]byte("secret"))))
	e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		Format: "method=${method}, uri=${uri}, status=${status}, latency_human=${latency_human}\n",
		Output: os.Stderr,
	}))
	e.Static("/", "public")
	e.GET("/", func(c echo.Context) error {
		events, err := getEvents(false)
		if err != nil {
			return err
		}
		for i, v := range events {
			events[i] = sanitizeEvent(v)
		}
		return c.Render(200, "index.tmpl", echo.Map{
			"events": events,
			"user":   c.Get("user"),
			"origin": c.Scheme() + "://" + c.Request().Host,
		})
	}, fillinUser)
	e.GET("/initialize", func(c echo.Context) error {
		// db reset
		cmd := exec.Command("../../db/init.sh")
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		err := cmd.Run()
		if err != nil {
			return nil
		}

		// go-cache reset
		goCache.Flush()
		{
			cacheSheets()
		}

		// redis reset
		{
			redisCli.FlushAll()
		}

		return c.NoContent(204)
	})
	e.POST("/api/users", func(c echo.Context) error {
		var params struct {
			Nickname  string `json:"nickname"`
			LoginName string `json:"login_name"`
			Password  string `json:"password"`
		}
		c.Bind(&params)

		tx, err := db.Begin()
		if err != nil {
			return err
		}

		var user User
		if err := tx.QueryRow("SELECT * FROM users WHERE login_name = ?", params.LoginName).Scan(&user.ID, &user.LoginName, &user.Nickname, &user.PassHash); err != sql.ErrNoRows {
			tx.Rollback()
			if err == nil {
				return resError(c, "duplicated", 409)
			}
			return err
		}

		res, err := tx.Exec("INSERT INTO users (login_name, pass_hash, nickname) VALUES (?, SHA2(?, 256), ?)", params.LoginName, params.Password, params.Nickname)
		if err != nil {
			tx.Rollback()
			return resError(c, "", 0)
		}
		userID, err := res.LastInsertId()
		if err != nil {
			tx.Rollback()
			return resError(c, "", 0)
		}
		if err := tx.Commit(); err != nil {
			return err
		}

		return c.JSON(201, echo.Map{
			"id":       userID,
			"nickname": params.Nickname,
		})
	})
	e.GET("/api/users/:id", func(c echo.Context) error {
		var user User
		if err := db.QueryRow("SELECT id, nickname FROM users WHERE id = ?", c.Param("id")).Scan(&user.ID, &user.Nickname); err != nil {
			return err
		}

		loginUser, err := getLoginUser(c)
		if err != nil {
			return err
		}
		if user.ID != loginUser.ID {
			return resError(c, "forbidden", 403)
		}

		rows, err := db.Query("SELECT r.*, s.rank AS sheet_rank, s.num AS sheet_num FROM reservations r INNER JOIN sheets s ON s.id = r.sheet_id WHERE r.user_id = ? ORDER BY IFNULL(r.canceled_at, r.reserved_at) DESC LIMIT 5", user.ID)
		if err != nil {
			return err
		}
		defer rows.Close()

		var recentReservations []Reservation
		for rows.Next() {
			var reservation Reservation
			var sheet Sheet
			if err := rows.Scan(&reservation.ID, &reservation.EventID, &reservation.SheetID, &reservation.UserID, &reservation.ReservedAt, &reservation.CanceledAt, &sheet.Rank, &sheet.Num); err != nil {
				return err
			}
			reservation.SheetRank = sheet.Rank
			reservation.SheetNum = sheet.Num
			reservation.ReservedAtUnix = reservation.ReservedAt.Unix()
			if reservation.CanceledAt != nil {
				reservation.CanceledAtUnix = reservation.CanceledAt.Unix()
			}
			recentReservations = append(recentReservations, reservation)
		}

		if recentReservations == nil {
			recentReservations = make([]Reservation, 0)
		} else {
			// eventまとめてfetch
			eventIDs := funk.Map(recentReservations, func(x Reservation) int64 {
				return x.EventID
			})

			// =========
			bfTime := time.Now()
			// =========

			events, err := getEventsIn(eventIDs.([]int64), -1)
			if err != nil {
				log.Fatal(err)
				return err
			}

			// =========
			afTime := time.Now()
			log.Printf("##### TIME: %f #####", afTime.Sub(bfTime).Seconds())
			// =========

			for i := range recentReservations {
				// 複数event取得後にアプリケーション側でFind
				found := funk.Find(events, func(x *Event) bool {
					return x.ID == recentReservations[i].EventID
				})
				foundEvent := found.(*Event)

				// 同じEventIDの場合あるので構造体のコピーが必要
				cloned := *foundEvent

				price := cloned.Sheets[recentReservations[i].SheetRank].Price
				cloned.Sheets = nil
				cloned.Total = 0
				cloned.Remains = 0

				recentReservations[i].Price = price
				recentReservations[i].Event = &cloned
			}
		}

		var totalPrice int
		if err := db.QueryRow("SELECT IFNULL(SUM(e.price + s.price), 0) FROM reservations r INNER JOIN sheets s ON s.id = r.sheet_id INNER JOIN events e ON e.id = r.event_id WHERE r.user_id = ? AND r.canceled_at IS NULL", user.ID).Scan(&totalPrice); err != nil {
			return err
		}

		rows, err = db.Query("SELECT event_id FROM reservations WHERE user_id = ? GROUP BY event_id ORDER BY MAX(IFNULL(canceled_at, reserved_at)) DESC LIMIT 5", user.ID)
		if err != nil {
			return err
		}
		defer rows.Close()

		var recentEvents []*Event
		for rows.Next() {
			var eventID int64
			if err := rows.Scan(&eventID); err != nil {
				return err
			}
			event, err := getEvent(eventID, -1)
			if err != nil {
				return err
			}
			for k := range event.Sheets {
				event.Sheets[k].Detail = nil
			}
			recentEvents = append(recentEvents, event)
		}
		if recentEvents == nil {
			recentEvents = make([]*Event, 0)
		}

		return c.JSON(200, echo.Map{
			"id":                  user.ID,
			"nickname":            user.Nickname,
			"recent_reservations": recentReservations,
			"total_price":         totalPrice,
			"recent_events":       recentEvents,
		})
	}, loginRequired)
	e.POST("/api/actions/login", func(c echo.Context) error {
		var params struct {
			LoginName string `json:"login_name"`
			Password  string `json:"password"`
		}
		c.Bind(&params)

		user := new(User)
		if err := db.QueryRow("SELECT * FROM users WHERE login_name = ?", params.LoginName).Scan(&user.ID, &user.LoginName, &user.Nickname, &user.PassHash); err != nil {
			if err == sql.ErrNoRows {
				return resError(c, "authentication_failed", 401)
			}
			return err
		}

		var passHash string
		if err := db.QueryRow("SELECT SHA2(?, 256)", params.Password).Scan(&passHash); err != nil {
			return err
		}
		if user.PassHash != passHash {
			return resError(c, "authentication_failed", 401)
		}

		sess.SessSetUserID(c, user.ID)
		user, err = getLoginUser(c)
		if err != nil {
			return err
		}
		return c.JSON(200, user)
	})
	e.POST("/api/actions/logout", func(c echo.Context) error {
		sess.SessDeleteUserID(c)
		return c.NoContent(204)
	}, loginRequired)
	e.GET("/api/events", func(c echo.Context) error {
		events, err := getEvents(true)
		if err != nil {
			return err
		}
		for i, v := range events {
			events[i] = sanitizeEvent(v)
		}
		return c.JSON(200, events)
	})
	e.GET("/api/events/:id", func(c echo.Context) error {
		eventID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			return resError(c, "not_found", 404)
		}

		loginUserID := int64(-1)
		if user, err := getLoginUser(c); err == nil {
			loginUserID = user.ID
		}

		event, err := getEvent(eventID, loginUserID)
		if err != nil {
			if err == sql.ErrNoRows {
				return resError(c, "not_found", 404)
			}
			return err
		} else if !event.PublicFg {
			return resError(c, "not_found", 404)
		}
		return c.JSON(200, sanitizeEvent(event))
	})
	e.POST("/api/events/:id/actions/reserve", func(c echo.Context) error {
		eventID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			return resError(c, "not_found", 404)
		}
		var params struct {
			Rank string `json:"sheet_rank"`
		}
		c.Bind(&params)

		user, err := getLoginUser(c)
		if err != nil {
			return err
		}

		event, err := getEvent(eventID, user.ID)
		if err != nil {
			if err == sql.ErrNoRows {
				return resError(c, "invalid_event", 404)
			}
			return err
		} else if !event.PublicFg {
			return resError(c, "invalid_event", 404)
		}

		if !validateRank(params.Rank) {
			return resError(c, "invalid_rank", 400)
		}

		var reservationID int64
		var sheet Sheet
		for {
			reservationID, sheet, err = tryInsertReservation(*user, *event, params.Rank)
			if err == sql.ErrNoRows {
				// レスポンスを返すエラー
				return resError(c, "sold_out", 409)
			} else if err == redis.TxFailedErr {
				// WATCH のエラー
				continue
			} else if err != nil {
				// その他
				log.Printf("##### INSERT ERROR ##### %v", err)
				continue
			} else {
				// 正常
				break
			}
		}

		return c.JSON(202, echo.Map{
			"id":         reservationID,
			"sheet_rank": params.Rank,
			"sheet_num":  sheet.Num,
		})
	}, loginRequired)
	e.DELETE("/api/events/:id/sheets/:rank/:num/reservation", func(c echo.Context) error {
		eventID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			return resError(c, "not_found", 404)
		}
		rank := c.Param("rank")
		num := c.Param("num")

		user, err := getLoginUser(c)
		if err != nil {
			return err
		}

		event, err := getEvent(eventID, user.ID)
		if err != nil {
			if err == sql.ErrNoRows {
				return resError(c, "invalid_event", 404)
			}
			return err
		} else if !event.PublicFg {
			return resError(c, "invalid_event", 404)
		}

		if !validateRank(rank) {
			return resError(c, "invalid_rank", 404)
		}

		var sheet Sheet
		if err := db.QueryRow("SELECT * FROM sheets WHERE `rank` = ? AND num = ?", rank, num).Scan(&sheet.ID, &sheet.Rank, &sheet.Num, &sheet.Price); err != nil {
			if err == sql.ErrNoRows {
				return resError(c, "invalid_sheet", 404)
			}
			return err
		}

		// tx, err := db.Begin()
		// if err != nil {
		// 	return err
		// }

		// WATCH
		key := "reservations.notCanceled.eid." + strconv.FormatInt(eventID, 10)
		err = redisCli.Watch(func(tx *redis.Tx) error {
			pipe := tx.TxPipeline()

			// fetch the first reserved record of the event
			reservations, err := fetchAndCacheReservations([]int64{event.ID}, pipe)
			if err != nil {
				log.Fatal(err)
				return err
			}
			found := funk.Find(reservations, func(x *Reservation) bool {
				return x.SheetID == sheet.ID
			})
			if found == nil {
				log.Printf("NOT FOUND (DELETE RESERVATIONS)")
				return resError(c, "not_reserved", 400)
			}
			reservation := *(found.(*Reservation))

			if reservation.UserID != user.ID {
				// tx.Rollback()
				log.Printf("403 (DELETE RESERVATIONS) RUID: %v, sessionUID: %v", reservation.UserID, user.ID)
				return resError(c, "not_permitted", 403)
			}

			if _, err := db.Exec("UPDATE reservations SET canceled_at = ? WHERE id = ?", time.Now().UTC().Format("2006-01-02 15:04:05.000000"), reservation.ID); err != nil {
				// tx.Rollback()
				log.Printf("ROLLBACK UPDATE reservations")
				return err
			}

			// update cache before commit DB
			{
				hdel(event.ID, reservation.ID)
			}

			_, err = pipe.Exec()
			if err != nil {
				return err
			}

			return err
		}, key)

		// if err := tx.Commit(); err != nil {
		// 	log.Printf("ERR when commiting UPDATE reservations")
		// 	return err
		// }

		return c.NoContent(204)
	}, loginRequired)
	e.GET("/admin/", func(c echo.Context) error {
		var events []*Event
		administrator := c.Get("administrator")
		if administrator != nil {
			var err error
			if events, err = getEvents(true); err != nil {
				return err
			}
		}
		return c.Render(200, "admin.tmpl", echo.Map{
			"events":        events,
			"administrator": administrator,
			"origin":        c.Scheme() + "://" + c.Request().Host,
		})
	}, fillinAdministrator)
	e.POST("/admin/api/actions/login", func(c echo.Context) error {
		var params struct {
			LoginName string `json:"login_name"`
			Password  string `json:"password"`
		}
		c.Bind(&params)

		administrator := new(Administrator)
		if err := db.QueryRow("SELECT * FROM administrators WHERE login_name = ?", params.LoginName).Scan(&administrator.ID, &administrator.LoginName, &administrator.Nickname, &administrator.PassHash); err != nil {
			if err == sql.ErrNoRows {
				return resError(c, "authentication_failed", 401)
			}
			return err
		}

		var passHash string
		if err := db.QueryRow("SELECT SHA2(?, 256)", params.Password).Scan(&passHash); err != nil {
			return err
		}
		if administrator.PassHash != passHash {
			return resError(c, "authentication_failed", 401)
		}

		sess.SessSetAdministratorID(c, administrator.ID)
		administrator, err = getLoginAdministrator(c)
		if err != nil {
			return err
		}
		return c.JSON(200, administrator)
	})
	e.POST("/admin/api/actions/logout", func(c echo.Context) error {
		sess.SessDeleteAdministratorID(c)
		return c.NoContent(204)
	}, adminLoginRequired)
	e.GET("/admin/api/events", func(c echo.Context) error {
		events, err := getEvents(true)
		if err != nil {
			return err
		}
		return c.JSON(200, events)
	}, adminLoginRequired)
	e.POST("/admin/api/events", func(c echo.Context) error {
		var params struct {
			Title  string `json:"title"`
			Public bool   `json:"public"`
			Price  int    `json:"price"`
		}
		c.Bind(&params)

		tx, err := db.Begin()
		if err != nil {
			return err
		}

		res, err := tx.Exec("INSERT INTO events (title, public_fg, closed_fg, price) VALUES (?, ?, 0, ?)", params.Title, params.Public, params.Price)
		if err != nil {
			tx.Rollback()
			return err
		}
		eventID, err := res.LastInsertId()
		if err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}

		event, err := getEvent(eventID, -1)
		if err != nil {
			return err
		}
		return c.JSON(200, event)
	}, adminLoginRequired)
	e.GET("/admin/api/events/:id", func(c echo.Context) error {
		eventID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			return resError(c, "not_found", 404)
		}
		event, err := getEvent(eventID, -1)
		if err != nil {
			if err == sql.ErrNoRows {
				return resError(c, "not_found", 404)
			}
			return err
		}
		return c.JSON(200, event)
	}, adminLoginRequired)
	e.POST("/admin/api/events/:id/actions/edit", func(c echo.Context) error {
		eventID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			return resError(c, "not_found", 404)
		}

		var params struct {
			Public bool `json:"public"`
			Closed bool `json:"closed"`
		}
		c.Bind(&params)
		if params.Closed {
			params.Public = false
		}

		event, err := getEvent(eventID, -1)
		if err != nil {
			if err == sql.ErrNoRows {
				return resError(c, "not_found", 404)
			}
			return err
		}

		if event.ClosedFg {
			return resError(c, "cannot_edit_closed_event", 400)
		} else if event.PublicFg && params.Closed {
			return resError(c, "cannot_close_public_event", 400)
		}

		tx, err := db.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE events SET public_fg = ?, closed_fg = ? WHERE id = ?", params.Public, params.Closed, event.ID); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}

		e, err := getEvent(eventID, -1)
		if err != nil {
			return err
		}
		c.JSON(200, e)
		return nil
	}, adminLoginRequired)
	e.GET("/admin/api/reports/events/:id/sales", func(c echo.Context) error {
		eventID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			return resError(c, "not_found", 404)
		}

		event, err := getEvent(eventID, -1)
		if err != nil {
			return err
		}

		rows, err := db.Query("SELECT r.*, s.rank AS sheet_rank, s.num AS sheet_num, s.price AS sheet_price, e.price AS event_price FROM reservations r INNER JOIN sheets s ON s.id = r.sheet_id INNER JOIN events e ON e.id = r.event_id WHERE r.event_id = ? ORDER BY reserved_at ASC", event.ID)
		if err != nil {
			return err
		}
		defer rows.Close()

		var reports []Report
		for rows.Next() {
			var reservation Reservation
			var sheet Sheet
			if err := rows.Scan(&reservation.ID, &reservation.EventID, &reservation.SheetID, &reservation.UserID, &reservation.ReservedAt, &reservation.CanceledAt, &sheet.Rank, &sheet.Num, &sheet.Price, &event.Price); err != nil {
				return err
			}
			report := Report{
				ReservationID: reservation.ID,
				EventID:       event.ID,
				Rank:          sheet.Rank,
				Num:           sheet.Num,
				UserID:        reservation.UserID,
				SoldAt:        reservation.ReservedAt.Format("2006-01-02T15:04:05.000000Z"),
				Price:         event.Price + sheet.Price,
			}
			if reservation.CanceledAt != nil {
				report.CanceledAt = reservation.CanceledAt.Format("2006-01-02T15:04:05.000000Z")
			}
			reports = append(reports, report)
		}
		return renderReportCSV(c, reports)
	}, adminLoginRequired)

	e.GET("/admin/api/reports/sales", func(c echo.Context) error {
		// ORDER BY すると、巨大な一時テーブル作るのでクソ遅い。しなくても、チェック通る、罠
		rows, err := db.Query("select r.id, r.user_id, r.reserved_at, r.canceled_at, s.rank as sheet_rank, s.num as sheet_num, s.price as sheet_price, e.id as event_id, e.price as event_price from reservations r inner join sheets s on s.id = r.sheet_id inner join events e on e.id = r.event_id")
		if err != nil {
			return err
		}
		defer rows.Close()

		var reports []Report
		for rows.Next() {
			var reservation Reservation
			var sheet Sheet
			var event Event
			if err := rows.Scan(&reservation.ID, &reservation.UserID, &reservation.ReservedAt, &reservation.CanceledAt, &sheet.Rank, &sheet.Num, &sheet.Price, &event.ID, &event.Price); err != nil {
				return err
			}
			report := Report{
				ReservationID: reservation.ID,
				UserID:        reservation.UserID,
				SoldAt:        reservation.ReservedAt.Format("2006-01-02T15:04:05.000000Z"),
				Rank:          sheet.Rank,
				Num:           sheet.Num,
				Price:         event.Price + sheet.Price,
				EventID:       event.ID,
			}
			if reservation.CanceledAt != nil {
				report.CanceledAt = reservation.CanceledAt.Format("2006-01-02T15:04:05.000000Z")
			}
			reports = append(reports, report)
		}
		return renderReportCSV(c, reports)
	}, adminLoginRequired)

	e.Logger.Fatal(e.Start(":8080"))
}

type Report struct {
	ReservationID int64
	EventID       int64
	Rank          string
	Num           int64
	UserID        int64
	SoldAt        string
	CanceledAt    string
	Price         int64
}

func renderReportCSV(c echo.Context, reports []Report) error {
	sort.Slice(reports, func(i, j int) bool { return strings.Compare(reports[i].SoldAt, reports[j].SoldAt) < 0 })

	body := bytes.NewBufferString("reservation_id,event_id,rank,num,price,user_id,sold_at,canceled_at\n")
	for _, v := range reports {
		body.WriteString(fmt.Sprintf("%d,%d,%s,%d,%d,%d,%s,%s\n",
			v.ReservationID, v.EventID, v.Rank, v.Num, v.Price, v.UserID, v.SoldAt, v.CanceledAt))
	}

	c.Response().Header().Set("Content-Type", `text/csv; charset=UTF-8`)
	c.Response().Header().Set("Content-Disposition", `attachment; filename="report.csv"`)
	_, err := io.Copy(c.Response(), body)
	return err
}

func resError(c echo.Context, e string, status int) error {
	if e == "" {
		e = "unknown"
	}
	if status < 100 {
		status = 500
	}
	return c.JSON(status, map[string]string{"error": e})
}
