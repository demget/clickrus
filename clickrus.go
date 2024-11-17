package clickrus

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/mailru/go-clickhouse"
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
		conn        *sql.DB
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

	conf.Columns = append([]string{
		"date",
		"time",
		"level",
		"message",
	}, conf.Columns...)

	db, err := sql.Open("clickhouse", conf.Addr)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(100)
	db.SetMaxOpenConns(100)

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
		conn:   db,
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

	result["date"] = entry.Time.UTC().Format("2006-01-02")
	result["time"] = entry.Time.UTC().Format("2006-01-02T15:04:05")
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

	var rows [][]interface{}
	for _, field := range fields {
		var row []interface{}
		for _, column := range h.Columns {
			v, ok := field[column]
			if !ok {
				row = append(row, "")
				continue
			}

			if _, ok := v.(logrus.Fields); ok {
				data, _ := json.Marshal(v)
				row = append(row, string(data))
			} else {
				row = append(row, fmt.Sprintf("%+v", v))
			}
		}
		rows = append(rows, row)
	}

	query, args, err := buildMultiInsert(h.Table, h.Columns, rows)
	if err != nil {
		return err
	}

	_, err = h.conn.Exec(query, args...)
	return err
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

func buildMultiInsert(tbl string, cols []string, rows [][]interface{}) (string, []interface{}, error) {
	var (
		stmt string
		args []interface{}
	)

	if len(cols) == 0 || len(rows) == 0 {
		return "", nil, errors.New("rows and cols cannot be empty")
	}

	colCount := len(cols)
	rowCount := len(rows)
	args = make([]interface{}, colCount*rowCount)
	argi := 0

	for _, row := range rows {
		if len(row) != colCount {
			return "", nil, errors.New("amount of row items does not match column count")
		}
		for _, val := range row {
			args[argi] = val
			argi++
		}
	}

	binds := strings.Repeat("?,", colCount)
	binds = "(" + binds[:len(binds)-1] + "),"
	batch := strings.Repeat(binds, rowCount)
	batch = batch[:len(batch)-1]

	stmt = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tbl, strings.Join(cols, ","), batch)

	return stmt, args, nil
}
