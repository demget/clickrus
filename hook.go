package hook

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/roistat/go-clickhouse"
	"github.com/sirupsen/logrus"
)

// log is the system logger. Does not participate in hook process.
// Can be redeclare in NewHook and NewAsyncHook.
var log = logrus.New()

func init() {
	log.Formatter = &logrus.JSONFormatter{}
	log.SetLevel(logrus.ErrorLevel)
	log.Out = os.Stderr
}

type (
	// ClickHouseConfig configures connection to clickhouse database.
	ClickHouseConfig struct {
		Host    string        `yaml:"host"`
		DB      string        `yaml:"db"`
		Table   string        `yaml:"table"`
		Columns []string      `yaml:"columns"`
		Timeout time.Duration `yaml:"timeout"`
	}

	// Config configures Hook and AsyncHook.
	Config struct {
		Stage        string           `yaml:"stage"`
		BufferSize   int              `yaml:"buffer_size"`
		TickerPeriod time.Duration    `yaml:"ticker_period"`
		Connection   ClickHouseConfig `yaml:"connection"`
		Levels       []string         `yaml:"levels"`
	}

	// Hook implements logrus.Hook interface for delivers logs to clickhouse
	// database.
	Hook struct {
		Config     *Config
		connection *clickhouse.Conn
		levels     []logrus.Level
	}

	// AsyncHook implements logrus.Hook interface for delivers logs to
	// clickhouse database. Creates batch and saves entry data in time ticker.
	AsyncHook struct {
		*Hook
		bus     chan map[string]interface{}
		flush   chan bool
		halt    chan bool
		flushWg *sync.WaitGroup
		Ticker  *time.Ticker
	}
)

// Validate checks required fields.
func (config *ClickHouseConfig) Validate() error {
	if config.Host == "" {
		return errors.New("host can not be empty")
	}

	if config.DB == "" {
		return errors.New("db can not be empty")
	}

	if config.Table == "" {
		return errors.New("table can not be empty")
	}

	return nil
}

// NewHook creates logrus hook to clickhouse.
func NewHook(config *Config, logger ...*logrus.Logger) (*Hook, error) {
	if len(logger) != 0 {
		log = logger[0]
	}

	conn, err := newClickHouseConn(&config.Connection)
	if err != nil {
		return nil, err
	}

	if err := exists(&config.Connection, conn); err != nil {
		return nil, err
	}

	levels, err := parseLevels(config.Levels)
	if err != nil {
		return nil, err
	}

	hook := &Hook{
		Config:     config,
		connection: conn,
		levels:     levels,
	}

	return hook, nil
}

// Fire saves entry to the database.
func (h *Hook) Fire(entry *logrus.Entry) error {
	h.addDefaultsToEntry(entry)

	return h.save(entry.Data)
}

// SetLevels sets log levels to Hook.
func (h *Hook) SetLevels(lvs []logrus.Level) {
	h.levels = lvs
}

// Levels implements logrus.Hook.
func (h *Hook) Levels() []logrus.Level {
	if h.levels == nil {
		return logrus.AllLevels
	}

	return h.levels
}

func (h *Hook) addDefaultsToEntry(entry *logrus.Entry) {
	if _, ok := entry.Data["msg"]; !ok {
		entry.Data["msg"] = entry.Message
	}

	entry.Data["time"] = entry.Time.UTC().Format("2006-01-02 15:04:05")
	entry.Data["date"] = entry.Time.UTC().Format("2006-01-02")
	entry.Data["level"] = entry.Level.String()
	if h.Config.Stage != "" {
		entry.Data["stage"] = h.Config.Stage
	}
}

func (h *Hook) save(field map[string]interface{}) error {
	rows := buildRows(h.Config.Connection.Columns, []map[string]interface{}{field})
	err := persist(h.Config, h.connection, rows)

	return err
}

// NewAsyncHook creates async logrus hook to clickhouse.
func NewAsyncHook(config *Config, logger ...*logrus.Logger) (*AsyncHook, error) {
	if len(logger) != 0 {
		log = logger[0]
	}

	conn, err := newClickHouseConn(&config.Connection)
	if err != nil {
		return nil, err
	}

	if err := exists(&config.Connection, conn); err != nil {
		return nil, err
	}

	levels, err := parseLevels(config.Levels)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup

	hook := &AsyncHook{
		Hook: &Hook{
			Config:     config,
			connection: conn,
			levels:     levels,
		},
		bus:     make(chan map[string]interface{}, config.BufferSize),
		flush:   make(chan bool),
		halt:    make(chan bool),
		flushWg: &wg,
		Ticker:  time.NewTicker(config.TickerPeriod),
	}

	go hook.fire()

	return hook, nil
}

// Fire adds entry records to batch.
func (h *AsyncHook) Fire(entry *logrus.Entry) error {
	h.addDefaultsToEntry(entry)
	h.bus <- entry.Data

	return nil
}

// SetLevels sets log levels to AsyncHook.
func (h *AsyncHook) SetLevels(lvs []logrus.Level) {
	h.levels = lvs
}

// Levels implements logrus.Hook.
func (h *AsyncHook) Levels() []logrus.Level {
	if h.levels == nil {
		return logrus.AllLevels
	}

	return h.levels
}

// Flush saves batch entry records to the database.
func (h *AsyncHook) Flush() {
	log.Debug("flush...")
	h.flushWg.Add(1)
	h.flush <- true
	h.flushWg.Wait()
}

// Close closes AsyncHook.
func (h *AsyncHook) Close() {
	log.Debug("close...")
	h.halt <- true
}

func (h *AsyncHook) saveBatch(fields []map[string]interface{}) error {
	rows := buildRows(h.Config.Connection.Columns, fields)
	err := persist(h.Config, h.connection, rows)

	return err
}

func (h *AsyncHook) fire() {
	var buffer []map[string]interface{}

	defer func() {
		if err := h.saveBatch(buffer); err != nil {
			log.Error(err)
		}
	}()

	for {
		select {
		case fields := <-h.bus:
			log.Debug("push message into bus...")
			buffer = append(buffer, fields)
			if len(buffer) >= h.Config.BufferSize {
				err := h.saveBatch(buffer)
				if err != nil {
					log.Error(err)
				}
				buffer = buffer[:0]
			}
			continue
		default:
		}

		select {
		case fields := <-h.bus:
			log.Debug("push message into bus...")
			buffer = append(buffer, fields)
			if len(buffer) >= h.Config.BufferSize {
				err := h.saveBatch(buffer)
				if err != nil {
					log.Error(err)
				}
				buffer = buffer[:0]
			}
		case <-h.Ticker.C:
			log.Debug("flush by ticker...")
			err := h.saveBatch(buffer)
			if err != nil {
				log.Error(err)
			}
			buffer = buffer[:0]
		case <-h.flush:
			log.Debug("flush by flush...")
			err := h.saveBatch(buffer)
			if err != nil {
				log.Error(err)
			}
			buffer = buffer[:0]
			h.flushWg.Done()
		case <-h.halt:
			log.Debug("halt...")
			h.Flush()
			return
		}
	}
}

func parseLevels(lvls []string) ([]logrus.Level, error) {
	var levels = make([]logrus.Level, 0, len(lvls))

	for _, lvl := range lvls {
		level, err := logrus.ParseLevel(lvl)
		if err != nil {
			return levels, err
		}
		levels = append(levels, level)
	}

	return levels, nil
}

func newClickHouseConn(config *ClickHouseConfig) (*clickhouse.Conn, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("hook config is invalid: %s", err)
	}

	httpTransport := clickhouse.NewHttpTransport()
	httpTransport.Timeout = config.Timeout
	conn := clickhouse.NewConn(config.Host, httpTransport)

	if err := conn.Ping(); err != nil {
		return nil, err
	}

	return conn, nil
}

func exists(config *ClickHouseConfig, conn *clickhouse.Conn) error {
	queryExists := fmt.Sprintf("EXISTS TABLE %s.%s", config.DB, config.Table)
	row := clickhouse.NewQuery(queryExists)

	var exists int8
	iter := row.Iter(conn)
	iter.Scan(&exists)
	if err := iter.Error(); err != nil {
		return err
	}

	if exists == 0 {
		return fmt.Errorf("table %s.%s does not exists", config.DB, config.Table)
	}

	return nil
}

func persist(config *Config, connection *clickhouse.Conn, rows clickhouse.Rows) error {
	if rows == nil || len(rows) == 0 {
		return nil
	}

	table := config.Connection.DB + "." + config.Connection.Table
	query, err := clickhouse.BuildMultiInsert(table, config.Connection.Columns, rows)

	if err != nil {
		return err
	}

	log.Debug("exec query")

	return query.Exec(connection)
}

func buildRows(columns []string, fields []map[string]interface{}) (rows clickhouse.Rows) {
	for _, field := range fields {
		row := clickhouse.Row{}

		for _, column := range columns {
			val, ok := field[column]
			if !ok {
				if isIntField(column) {
					val = 0
				} else {
					val = ""
				}
			}

			row = append(row, val)
		}

		rows = append(rows, row)
	}

	return
}

func isIntField(name string) bool {
	if name == "cnt" || strings.Contains(name, "_id") {
		return true
	}
	return false
}
