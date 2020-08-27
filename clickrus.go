package clickrus

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/roistat/go-clickhouse"
	"github.com/sirupsen/logrus"
)

type (
	// Config configures connection to the clickhouse database.
	Config struct {
		Logger     *logrus.Logger `yaml:"-"`
		Addr       string         `yaml:"host"`
		Table      string         `yaml:"table"`
		Period     time.Duration  `yaml:"period"`
		BufferSize int            `yaml:"buffer_size"`
		Columns    []string       `yaml:"columns"`
		Levels     []string       `yaml:"levels"`
	}

	// Hook implements logrus.Hook interface for delivering logs to the
	// clickhouse database. Creates batch and saves entry data in time ticker.
	Hook struct {
		Config
		levels      []logrus.Level
		wg          sync.WaitGroup
		conn        *clickhouse.Conn
		ticker      *time.Ticker
		bus         chan map[string]interface{}
		flush, halt chan bool
	}
)

// NewHook creates async logrus hook to clickhouse.
func NewHook(conf Config) (*Hook, error) {
	if conf.Addr == "" || conf.Table == "" {
		return nil, errors.New("clickrus: clickhouse data is invalid")
	}
	if conf.Logger == nil {
		conf.Logger = logrus.New()
	}
	if conf.BufferSize == 0 {
		conf.BufferSize = 32 * 1024
	}
	if conf.Period == 0 {
		conf.Period = 10 * time.Second
	}
	if conf.Levels == nil {
		conf.Levels = []string{"info", "error"}
	}
	if conf.Columns == nil {
		conf.Columns = []string{"date", "time", "level", "message"}
	}

	conn := clickhouse.NewConn(conf.Addr, clickhouse.HttpTransport{Timeout: 5 * time.Second})
	if err := conn.Ping(); err != nil {
		return nil, err
	}

	var exists int8
	q := clickhouse.NewQuery(fmt.Sprintf("EXISTS TABLE %s", conf.Table))
	q.Iter(conn).Scan(&exists)

	if exists == 0 {
		return nil, fmt.Errorf("clickrus: table %s does not exist", conf.Table)
	}

	var levels []logrus.Level
	for _, lvl := range conf.Levels {
		level, err := logrus.ParseLevel(lvl)
		if err != nil {
			return nil, err
		}
		levels = append(levels, level)
	}

	hook := &Hook{
		Config: conf,
		conn:   conn,
		levels: levels,
		ticker: time.NewTicker(conf.Period),
		bus:    make(chan map[string]interface{}, conf.BufferSize),
		flush:  make(chan bool),
		halt:   make(chan bool),
	}

	go hook.startTicker()
	return hook, nil
}

// Fire adds entry records to batch.
func (h *Hook) Fire(entry *logrus.Entry) error {
	result := make(map[string]interface{})
	if entry.Data != nil {
		for k, v := range entry.Data {
			k = strings.Replace(k, "-", "_", -1)
			if err, ok := v.(error); k == logrus.ErrorKey && v != nil && ok {
				result[k] = err.Error()
			} else {
				result[k] = v
			}
		}
	}

	result["date"] = entry.Time.Format("2006-01-02")
	result["time"] = entry.Time.Format("2006-01-02T15:04:05")
	result["message"] = entry.Message
	result["level"] = entry.Level.String()

	h.bus <- result
	return nil
}

// SetLevels sets log levels to Hook.
func (h *Hook) SetLevels(lvls []logrus.Level) {
	h.levels = lvls
}

// Levels implements logrus.Hook.
func (h *Hook) Levels() []logrus.Level {
	if h.levels == nil {
		return logrus.AllLevels
	}
	return h.levels
}

// Flush saves batch entry records to the database.
func (h *Hook) Flush() {
	h.wg.Add(1)
	h.flush <- true
	h.wg.Wait()
}

// Close closes Hook.
func (h *Hook) Close() {
	h.halt <- true
}

func (h *Hook) saveBatch(fields []map[string]interface{}) error {
	if len(fields) == 0 {
		return nil
	}

	var rows clickhouse.Rows
	for _, field := range fields {
		var row clickhouse.Row
		for _, column := range h.Columns {
			val, ok := field[column]
			if !ok {
				row = append(row, "")
			} else {
				row = append(row, fmt.Sprintf("%+v", val))
			}
		}
		rows = append(rows, row)
	}

	query, err := clickhouse.BuildMultiInsert(h.Table, h.Columns, rows)
	if err != nil {
		return err
	}

	return query.Exec(h.conn)
}

func (h *Hook) startTicker() {
	var buffer []map[string]interface{}

	defer func() {
		if err := h.saveBatch(buffer); err != nil {
			h.Logger.Error(err)
		}
	}()

	for {
		select {
		case fields := <-h.bus:
			buffer = append(buffer, fields)
			if len(buffer) >= h.Config.BufferSize {
				err := h.saveBatch(buffer)
				if err != nil {
					h.Logger.Error(err)
				}
				buffer = buffer[:0]
			}
		case <-h.ticker.C:
			err := h.saveBatch(buffer)
			if err != nil {
				h.Logger.Error(err)
			}
			buffer = buffer[:0]
		case <-h.flush:
			err := h.saveBatch(buffer)
			if err != nil {
				h.Logger.Error(err)
			}
			buffer = buffer[:0]
			h.wg.Done()
		case <-h.halt:
			h.Flush()
			return
		}
	}
}
