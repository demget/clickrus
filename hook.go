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
	ClickHouseConfig struct {
		Host    string        `yaml:"host"`
		DB      string        `yaml:"db"`
		Table   string        `yaml:"table"`
		Columns []string      `yaml:"columns"`
		Timeout time.Duration `yaml:"timeout"`
		// TODO: use me
		Credentials struct {
			User     string `yaml:"user"`
			Password string `yaml:"password"`
		} `yaml:"credentials"`
	}

	Config struct {
		Stage        string           `yaml:"stage"`
		BufferSize   int              `yaml:"buffer_size"`
		TickerPeriod time.Duration    `yaml:"ticker_period"`
		Connection   ClickHouseConfig `yaml:"connection"`
		Levels       []string         `yaml:"levels"`
	}

	Hook struct {
		Config     *Config
		connection *clickhouse.Conn
		levels     []logrus.Level
	}

	AsyncHook struct {
		*Hook
		bus     chan map[string]interface{}
		flush   chan bool
		halt    chan bool
		flushWg *sync.WaitGroup
		Ticker  *time.Ticker
	}
)

func (config *ClickHouseConfig) Validate() error {
	if len(config.Host) == 0 {
		return errors.New("host can not be empty")
	}

	if len(config.DB) == 0 {
		return errors.New("db can not be empty")
	}

	if len(config.Table) == 0 {
		return errors.New("table can not be empty")
	}

	return nil
}

func newClickHouseConn(config *ClickHouseConfig) (*clickhouse.Conn, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("hook config is invalid: %s", err)
	}

	httpTransport := clickhouse.NewHttpTransport()
	httpTransport.Timeout = config.Timeout
	conn := clickhouse.NewConn(config.Host, httpTransport)

	//params := url.Values{}
	//params.Add("user", config.Credentials.User)
	//params.Add("password", config.Credentials.Password)
	//conn.SetParams(params)

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
			if _, ok := field[column]; !ok {
				// TODO: refactor me
				if strings.Index(column, "_id") != -1 || strings.Index(column, "cnt") != -1 {
					field[column] = 0
				} else {
					field[column] = ""
				}
			}

			row = append(row, field[column])
		}

		rows = append(rows, row)
	}

	return
}

func ParseLevels(lvls []string) ([]logrus.Level, error) {
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

// NewHook creates logrus hooker to clickhouse
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

	levels, err := ParseLevels(config.Levels)
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

func (hook *Hook) addDefaultsToEntry(entry *logrus.Entry) {
	if _, ok := entry.Data["msg"]; !ok {
		entry.Data["msg"] = entry.Message
	}

	entry.Data["time"] = entry.Time.UTC().Format("2006-01-02 15:04:05")
	entry.Data["date"] = entry.Time.UTC().Format("2006-01-02")
	entry.Data["level"] = entry.Level.String()
	if len(hook.Config.Stage) != 0 {
		entry.Data["stage"] = hook.Config.Stage
	}
}

func (hook *Hook) Save(field map[string]interface{}) error {
	rows := buildRows(hook.Config.Connection.Columns, []map[string]interface{}{field})
	err := persist(hook.Config, hook.connection, rows)

	return err
}

func (hook *Hook) Fire(entry *logrus.Entry) error {
	hook.addDefaultsToEntry(entry)

	return hook.Save(entry.Data)
}

func (hook *Hook) SetLevels(lvs []logrus.Level) {
	hook.levels = lvs
}

func (hook *Hook) Levels() []logrus.Level {
	if hook.levels == nil {
		return []logrus.Level{
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
			logrus.WarnLevel,
			logrus.InfoLevel,
			logrus.DebugLevel,
		}
	}

	return hook.levels
}

// NewAsyncHook creates async logrus hooker to clickhouse
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

	levels, err := ParseLevels(config.Levels)
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

func (hook *AsyncHook) SaveBatch(fields []map[string]interface{}) error {
	rows := buildRows(hook.Config.Connection.Columns, fields)
	err := persist(hook.Config, hook.connection, rows)

	return err
}

func (hook *AsyncHook) Fire(entry *logrus.Entry) error {
	hook.addDefaultsToEntry(entry)
	hook.bus <- entry.Data

	return nil
}

func (hook *AsyncHook) SetLevels(lvs []logrus.Level) {
	hook.levels = lvs
}

func (hook *AsyncHook) Levels() []logrus.Level {
	if hook.levels == nil {
		return []logrus.Level{
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
			logrus.WarnLevel,
			logrus.InfoLevel,
			logrus.DebugLevel,
		}
	}

	return hook.levels
}

func (hook *AsyncHook) Flush() {
	log.Debug("flush...")
	hook.flushWg.Add(1)
	hook.flush <- true
	hook.flushWg.Wait()
}

func (hook *AsyncHook) Close() {
	log.Debug("close...")
	hook.halt <- true
}

func (hook *AsyncHook) fire() {
	var buffer []map[string]interface{}

	defer hook.SaveBatch(buffer)

	for {
		select {
		case fields := <-hook.bus:
			log.Debug("push message into bus...")
			buffer = append(buffer, fields)
			if len(buffer) >= hook.Config.BufferSize {
				err := hook.SaveBatch(buffer)
				if err != nil {
					log.Error(err)
				}
				buffer = buffer[:0]
			}
			continue
		default:
		}

		select {
		case fields := <-hook.bus:
			log.Debug("push message into bus...")
			buffer = append(buffer, fields)
			if len(buffer) >= hook.Config.BufferSize {
				err := hook.SaveBatch(buffer)
				if err != nil {
					log.Error(err)
				}
				buffer = buffer[:0]
			}
		case <-hook.Ticker.C:
			log.Debug("flush by ticker...")
			err := hook.SaveBatch(buffer)
			if err != nil {
				log.Error(err)
			}
			buffer = buffer[:0]
		case <-hook.flush:
			log.Debug("flush by flush...")
			err := hook.SaveBatch(buffer)
			if err != nil {
				log.Error(err)
			}
			buffer = buffer[:0]
			hook.flushWg.Done()
		case <-hook.halt:
			log.Debug("halt...")
			hook.Flush()
			return
		}

	}
}
