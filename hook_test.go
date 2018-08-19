package hook

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/roistat/go-clickhouse"
	"github.com/sirupsen/logrus"
)

var conn *clickhouse.Conn

func init() {
	config := newConfig()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	log.Formatter = &logrus.JSONFormatter{}

	var err error
	conn, err = newClickHouseConn(&config.Connection)
	if err != nil {
		log.Error(err)
		os.Exit(-1)
	}
}

func TestHook(t *testing.T) {
	config := newConfig()

	log := logrus.New()
	log.Out = ioutil.Discard

	if err := migrationUp(config); err != nil {
		t.Errorf("error when initialization hook: %s", err)
		return
	}

	hook, err := NewHook(config)
	if err != nil {
		t.Errorf("error when initialization hook: %s", err)
		return
	}

	log.AddHook(hook)

	for i := 0; i < 100; i++ {
		log.WithFields(logrus.Fields{"event_uuid": "e77c6fbd-c590-4459-870c-0a1db0dc35c7"}).Info("meow")
		log.WithFields(logrus.Fields{"event_uuid": "067f7e90-ff6f-4a0b-9c30-7ffed1dbb470"}).Warn("meow")
		log.WithFields(logrus.Fields{"event_uuid": "a5cd79f5-8876-4462-b093-66760712eb34"}).Error("meow")
	}

	if err := migrationDown(config); err != nil {
		t.Errorf("error when initialization hook: %s", err)
		return
	}
}

func TestAsyncHook(t *testing.T) {
	config := newConfig()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	log.Formatter = &logrus.JSONFormatter{}

	if err := migrationUp(config); err != nil {
		t.Errorf("error when initialization hook: %s", err)
		return
	}

	hook, err := NewAsyncHook(config, log)
	if err != nil {
		t.Errorf("error when initialization hook: %s", err)
	}

	defer func() {
		hook.Flush()
		hook.Close()

		if err := migrationDown(config); err != nil {
			t.Errorf("error when initialization hook: %s", err)
			return
		}
	}()

	logger := logrus.New()
	logger.Formatter = &logrus.JSONFormatter{}
	logger.Out = ioutil.Discard
	logger.AddHook(hook)

	for i := 0; i < 1000; i++ {
		logger.WithFields(logrus.Fields{"event_uuid": "e77c6fbd-c590-4459-870c-0a1db0dc35c7", "user_id": i}).Info("meow")
		logger.WithFields(logrus.Fields{"event_uuid": "067f7e90-ff6f-4a0b-9c30-7ffed1dbb470", "user_id": i}).Warning("meow")
		logger.WithFields(logrus.Fields{"event_uuid": "a5cd79f5-8876-4462-b093-66760712eb34", "user_id": i}).Error("meow")
	}
}

func newConfig() *Config {
	config := &Config{
		Stage:        "test-service",
		BufferSize:   32768,
		TickerPeriod: time.Second * 10,
		Connection: ClickHouseConfig{
			Host:    "clickhouse-server:8123",
			DB:      "logs_test",
			Table:   "test",
			Timeout: time.Duration(time.Second * 5),
			Columns: []string{
				"level",
				"msg",
				"date",
				"time",
				"stage",
				"event_uuid",
				"user_id",
			},
			Credentials: struct {
				User     string `yaml:"user"`
				Password string `yaml:"password"`
			}{
				User: "default",
			},
		},
		Levels: []string{
			"error",
			"warn",
			"warning",
			"info",
		},
	}

	return config
}

func migrationUp(config *Config) error {
	query := clickhouse.NewQuery(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", config.Connection.DB))
	if err := query.Exec(conn); err != nil {
		return err
	}

	query = clickhouse.NewQuery(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
	  level String,
	  msg String,
	  date Date,
	  time DateTime,
	  stage String,
	  event_uuid String,
	  user_id UInt32
	) ENGINE = MergeTree(date, (level, stage, event_uuid, user_id), 8192);`, config.Connection.DB, config.Connection.Table))
	if err := query.Exec(conn); err != nil {
		return err
	}

	return nil
}

func migrationDown(config *Config) error {
	query := clickhouse.NewQuery(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", config.Connection.DB))
	if err := query.Exec(conn); err != nil {
		return err
	}

	return nil
}
