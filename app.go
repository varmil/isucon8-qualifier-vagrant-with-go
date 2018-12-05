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
 * ## MySQL
 * sudo pkill mysql
 * sudo -u mysql /bin/sh /usr/bin/mysqld_safe --basedir=/usr &
 *
 *
 * ### SCORE LOG
 *   800  : SELECT FOR UPDATE が無駄に見えたので削除。
 *  4810  : getEvent() の reservations テーブルSELECTに関して N+1を解決。reservations.user_id に INDEX追加
 *  7700  : go-cacheを用いてsheetsをslice格納してキャッシュ。この時点でgetEvent()はもはやボトルネックではないが、まだ /api/events/:id/actions/reserve 自体は遅い。
 *  9510  : getEvents()が内部で大量にgetEvent()をcallしていたので、呼出回数を実質１回にした。 /api/users/:id が次のボトルネックっぽい。
 * 26078  : WIP getEventsInをcacheしたがrace conditionで弾かれるので、ロックを入れないと駄目かも
 * 19292  : WIP ↑引き続き。Cache更新タイミングをトランザクションのcommit後に変更することで若干緩和。/actions/reservations のFOR UPDATEを削除
 * 9k-29k : WIP ↑引き続き。 myCache.FetchAndCacheReservations() に順次置き換え
 * 37k    : WIP ↑引き続き。 マニュアルをもう一度読む。/admin/api/reports/sales が原因で負荷レベルが上がらない。ORDER BYせずとも何とPASSした。罠。
 *        : refactor goCache --> redis
 *        : refactor WATCH（SETNX）を使うか、HASH型にしてATOMICに書き換えできるようにする。
 * 25k    : INSERT時にSETNXでLockを入れる。SheetsCache利用。早めにLock解除。まだ不安定
 * 18k    : 更に速く正確にLOCK解除するためにINSERT時にReservationIDを手動で採番する。
 * 45k    : Vagrantfileをいじって、本番スペックに近づけた。UPDATE時のWATCH排除。まだ不安定。
 * 30-55k : /admin/api/reports/sales で go func を利用。rows.Scan がまだ遅い？
 * 40-68k : /admin/api/reports/sales : funk.Find() が遅すぎるのでMapにした。まだ遅い （win）
 * 25-45k : /admin/api/reports/sales : canceledReservationsをredisからオンメモリにした。次は /admin/ or /actions/reserve （mac）
 * 35-45k : /admin/          : go func() で addEventInfo() をtuning（mac）
 * 55k    : /actions/reserve : Mutex Lock を使わずにAtomic Queueで頑張った。MySQLのmax_connectionsを増やした。 http://wakariyasui.hatenablog.jp/entry/2015/04/28/005109
 * 68k    : /                : getEventsIn()を細かく並列化したりループの回数を減らしたり。
 * 69k    : /                : MySQLのbuffer_pool_sizeを128MB --> 1G、 max_connectionsを400へ。
 *
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
	"sync/atomic"
	"time"

	"github.com/foize/go.fifo"
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

	myCache "torb/cache"
	sess "torb/session"
	. "torb/structs"
)

func arrayToString(a []int64, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
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

	// { eventID: { sheetRank: Queue of []sheet } }
	// set random sheetMap for new reservation
	{
		const EventLen = int64(18)
		const MaxEventLen = int64(100)

		sheets := funk.Shuffle(sheets).([]Sheet)
		data := map[int64]map[string]*fifo.Queue{}

		// 初期イベントは18個。11,12,13は全部空き。それ以外は全部埋まり。
		// 100くらいまで作っておく
		for eid := int64(1); eid <= MaxEventLen; eid++ {
			// initialize the map
			sheetMap := map[string]*fifo.Queue{
				"S": fifo.NewQueue(),
				"A": fifo.NewQueue(),
				"B": fifo.NewQueue(),
				"C": fifo.NewQueue(),
			}
			// set the map
			data[eid] = sheetMap
			// append if the event has non reserved sheets
			if (eid >= 11 && eid <= 13) || eid > EventLen {
				for _, s := range sheets {
					sheetMap[s.Rank].Add(s)
				}
			}
		}
		goCache.Set("randomSheetMap", data, cache.DefaultExpiration)
	}
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

	ids := funk.Map(events, func(x *Event) int64 {
		return x.ID
	})
	addedEvents, err := getEventsIn(ids.([]int64), -1)
	for i := range addedEvents {
		for k := range addedEvents[i].Sheets {
			addedEvents[i].Sheets[k].Detail = nil
		}
	}

	return addedEvents, nil
}

func getEventsIn(eventIDs []int64, loginUserID int64) ([]*Event, error) {
	var events []*Event
	inClause := arrayToString(eventIDs, ",")
	// log.Print(inClause)

	// EVENTS
	{
		// =========
		// bfTime := time.Now()
		// =========

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

		// =========
		// afTime := time.Now()
		// log.Printf("##### FETCHALLEVENT TIME: %f #####", afTime.Sub(bfTime).Seconds())
		// =========
	}

	// RESERVATIONS
	var reservations []*Reservation
	{
		// =========
		// bfTime := time.Now()
		// =========

		var err error
		reservations, err = myCache.FetchAndCacheReservations(db, eventIDs)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		// =========
		// afTime := time.Now()
		// log.Printf("##### [getEventsIn] RESERVATIONS TIME: %f #####", afTime.Sub(bfTime).Seconds())
		// =========
	}

	// ADD INFO
	{
		// =========
		// bfTime := time.Now()
		// =========

		// slice to map[eventID][]*Reservation
		data := map[int64][]*Reservation{}
		for _, r := range reservations {
			data[r.EventID] = append(data[r.EventID], r)
		}

		var wg sync.WaitGroup
		wg.Add(len(events))

		for i := range events {
			go func(i int) {
				defer wg.Done()
				err := addEventInfo(events[i], data[events[i].ID], loginUserID)
				if err != nil {
					panic(err)
				}
			}(i)
		}

		wg.Wait()
		// =========
		// afTime := time.Now()
		// log.Printf("##### [getEventsIn] ADD_EVENT_INFO TIME: %f #####", afTime.Sub(bfTime).Seconds())
		// =========
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
		reservations, err = myCache.FetchAndCacheReservations(db, []int64{eventID})
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

func addEventInfo(event *Event, reservations []*Reservation, loginUserID int64) error {
	// ----- sheets テーブルは不変なのでキャッシュしている -----
	var sheets []Sheet
	if x, found := goCache.Get("sheetsSlice"); found {
		sheets = x.([]Sheet)
	} else {
		log.Fatal("SHEETS CACHE NOT FOUND")
	}

	var wg sync.WaitGroup
	wg.Add(len(sheets))
	mx := new(sync.Mutex)

	for i, sheet := range sheets {
		go func(i int, sheet Sheet) {
			defer wg.Done()

			sheetCopy := Sheet{
				ID:    sheet.ID,
				Rank:  sheet.Rank,
				Num:   sheet.Num,
				Price: sheet.Price,
			}

			event.Sheets[sheetCopy.Rank].Price = event.Price + sheetCopy.Price
			atomic.AddInt32(&event.Total, 1)
			atomic.AddInt32(&event.Sheets[sheetCopy.Rank].Total, 1)

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
				atomic.AddInt32(&event.Remains, 1)
				atomic.AddInt32(&event.Sheets[sheetCopy.Rank].Remains, 1)
			} else {
				// シートに予約が入っている場合、最もReservedAtが早いものを取得
				sheetCopy.Mine = reservation.UserID == loginUserID
				sheetCopy.Reserved = true
				sheetCopy.ReservedAtUnix = reservation.ReservedAtUnix
			}

			mx.Lock()
			event.Sheets[sheetCopy.Rank].Detail = append(event.Sheets[sheetCopy.Rank].Detail, &sheetCopy)
			mx.Unlock()
		}(i, sheet)
	}

	wg.Wait()

	// =========
	// bfTime := time.Now()
	// =========

	wg.Add(4)
	for rank, sheets := range event.Sheets {
		go func(rank string, sheets *Sheets) {
			defer wg.Done()
			sort.Slice(sheets.Detail, func(i, j int) bool { return sheets.Detail[i].ID < sheets.Detail[j].ID })
		}(rank, sheets)
	}
	wg.Wait()

	// =========
	// afTime := time.Now()
	// log.Printf("##### [getEventsIn] SSS TIME: %f #####", afTime.Sub(bfTime).Seconds())
	// =========

	return nil
}

/**
 * INSERT INTO reservations
 */
func tryInsertReservation(user *User, event *Event, rank string) (int64, Sheet, error) {
	var reservationID int64
	var sheet Sheet

	x, _ := goCache.Get("randomSheetMap")
	sheetMap := x.(map[int64]map[string]*fifo.Queue)

	// ロックを使わないためにスレッドセーフなQueueを使ってAtomicに空席をPopする
	// try to pop non-reserved sheet id from the queue
	item := sheetMap[event.ID][rank].Next()
	if item == nil {
		return 0, Sheet{}, sql.ErrNoRows
	}
	sheet = item.(Sheet)

	atomic.AddInt64(&reservationUUID, 1)
	reservationID = atomic.LoadInt64(&reservationUUID)
	utcTime := time.Now().UTC()
	{
		reservation := Reservation{ID: reservationID, EventID: event.ID, SheetID: sheet.ID, UserID: user.ID, ReservedAt: &utcTime, ReservedAtUnix: utcTime.Unix()}
		myCache.HSet(event.ID, reservationID, &reservation)
	}

	_, err := db.Exec("INSERT INTO reservations (id, event_id, sheet_id, user_id, reserved_at) VALUES (?, ?, ?, ?, ?)", reservationID, event.ID, sheet.ID, user.ID, utcTime.Format("2006-01-02 15:04:05.000000"))
	if err != nil {
		return 0, Sheet{}, err
	}

	return reservationID, sheet, nil
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
var canceledRMX *sync.Mutex
var insertRMX *sync.Mutex
var redisCli *redis.Client
var json = jsoniter.ConfigCompatibleWithStandardLibrary

var reservationUUID int64 = 10000000
var ErrCantAcquireLock = errors.New("cant acquire lock")

// cache
var canceledReservations []*Reservation

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
		redisCli = myCache.CreateRedisClient()
		redisCli.FlushAll()
	}

	// go-cache
	{
		goCache = cache.New(60*time.Minute, 120*time.Minute)
		cacheSheets()
	}

	// mutex
	canceledRMX = new(sync.Mutex)
	insertRMX = new(sync.Mutex)

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
		{
			goCache.Flush()
			cacheSheets()
		}

		// redis reset
		{
			redisCli.FlushAll()
		}

		// cache canceled reservations
		{
			canceledReservations = nil

			rows, err := db.Query("select id, event_id, user_id, sheet_id, reserved_at, canceled_at from reservations where canceled_at is not null")
			if err != nil {
				return err
			}
			defer rows.Close()

			var reservations []*Reservation
			for rows.Next() {
				var reservation Reservation
				if err := rows.Scan(&reservation.ID, &reservation.EventID, &reservation.UserID, &reservation.SheetID, &reservation.ReservedAt, &reservation.CanceledAt); err != nil {
					return err
				}
				reservation.ReservedAtUnix = reservation.ReservedAt.Unix()
				reservation.CanceledAtUnix = reservation.CanceledAt.Unix()
				reservations = append(reservations, &reservation)
			}
			canceledReservations = append(canceledReservations, reservations...)
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
			// bfTime := time.Now()
			// =========

			events, err := getEventsIn(eventIDs.([]int64), -1)
			if err != nil {
				log.Fatal(err)
				return err
			}

			// =========
			// afTime := time.Now()
			// log.Printf("##### TIME: %f #####", afTime.Sub(bfTime).Seconds())
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

	// TODO: tuning
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
		reservationID, sheet, err = tryInsertReservation(user, event, params.Rank)
		if err == sql.ErrNoRows {
			// レスポンスを返すエラー
			return resError(c, "sold_out", 409)
		} else if err != nil {
			return err
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

		{
			// fetch the first reserved record of the event
			reservations, err := myCache.FetchAndCacheReservations(db, []int64{event.ID})
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
				log.Printf("403 (DELETE RESERVATIONS) RUID: %v, sessionUID: %v", reservation.UserID, user.ID)
				return resError(c, "not_permitted", 403)
			}

			canceledAt := time.Now().UTC()

			// update cache before commit DB
			{
				// append to non-reserved sheets cache
				x, _ := goCache.Get("randomSheetMap")
				sheetMap := x.(map[int64]map[string]*fifo.Queue)
				sheetMap[event.ID][rank].Add(sheet)

				// delete notCanceledReservations cache
				myCache.HDel(event.ID, reservation.ID)

				// sales用なので多少遅れても良さそう
				// append to canceledReservations cache
				reservation.CanceledAt = &canceledAt
				reservation.CanceledAtUnix = canceledAt.Unix()
				canceledRMX.Lock()
				canceledReservations = append(canceledReservations, &reservation)
				canceledRMX.Unlock()
			}

			if _, err := db.Exec("UPDATE reservations SET canceled_at = ? WHERE id = ?", canceledAt.Format("2006-01-02 15:04:05.000000"), reservation.ID); err != nil {
				return err
			}
		}

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

		// 本来ここでrandomSheetMap（go-cache）にINSERTすべきだが、初期化時にズルして
		// 余分にイベント作成しているのでここでは何もしなくてOKのはず

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

		// NOTE: Closedに変わったとしてもCacheは更新しない（あくまで予約席のキャッシュなので）
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
		return renderReportCSV(c, &reports)
	}, adminLoginRequired)

	e.GET("/admin/api/reports/sales", func(c echo.Context) error {
		// get cache of sheets
		var sheetsMap map[int64]Sheet
		if x, found := goCache.Get("sheetsSlice"); found {
			sheets := x.([]Sheet)
			sheetsMap = funk.Map(sheets, func(x Sheet) (int64, Sheet) {
				return x.ID, x
			}).(map[int64]Sheet)
		}

		var reservations []*Reservation
		events := map[int64]Event{}
		{
			rows, _ := db.Query("select id, price from events")
			defer rows.Close()
			for rows.Next() {
				var eid int64
				var price int64
				if err := rows.Scan(&eid, &price); err != nil {
					return err
				}
				e := Event{
					ID:    eid,
					Price: price,
				}
				events[eid] = e
			}

			// キャンセルしてないもの、しているものすべてを取得する
			for key := range events {
				notCanceled, _ := myCache.GetReservations(key)
				if len(notCanceled) > 0 {
					reservations = append(reservations, notCanceled...)
				}
			}

			canceled := canceledReservations
			if len(canceled) > 0 {
				reservations = append(reservations, canceled...)
			}
		}

		var wg sync.WaitGroup
		var reports = make([]Report, len(reservations))
		wg.Add(len(reservations))

		for i, reservation := range reservations {
			go func(reservation *Reservation, loopIndex int) {
				defer wg.Done()

				// get from map
				event := events[reservation.EventID]
				sheet := sheetsMap[reservation.SheetID]

				report := Report{
					ReservationID: reservation.ID,
					UserID:        reservation.UserID,
					SoldAt:        time.Unix(reservation.ReservedAtUnix, 0).Format("2006-01-02T15:04:05.000000Z"),
					Rank:          sheet.Rank,
					Num:           sheet.Num,
					Price:         event.Price + sheet.Price,
					EventID:       event.ID,
				}
				if reservation.CanceledAt != nil {
					report.CanceledAt = reservation.CanceledAt.Format("2006-01-02T15:04:05.000000Z")
				} else if reservation.CanceledAtUnix != 0 {
					report.CanceledAt = time.Unix(reservation.CanceledAtUnix, 0).Format("2006-01-02T15:04:05.000000Z")
				}
				reports[loopIndex] = report
			}(reservation, i)
		}

		wg.Wait()

		foo := renderReportCSV(c, &reports)
		return foo
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

func renderReportCSV(c echo.Context, reports *[]Report) error {
	// ソートなしでもOKだった、、、罠
	// sort.Slice(*reports, func(i, j int) bool { return strings.Compare((*reports)[i].SoldAt, (*reports)[j].SoldAt) < 0 })

	body := bytes.NewBufferString("reservation_id,event_id,rank,num,price,user_id,sold_at,canceled_at\n")
	for _, v := range *(reports) {
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
