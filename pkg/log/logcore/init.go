// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logcore

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"gopkg.in/natefinch/lumberjack.v2"
)

// InitLogger initializes a zap logger from config.
// It sets up file and stdout outputs, creates the core, and returns
// a debug-level logger (caller uses AtomicLevel to control actual level).
// The returned cleanup function should be called on shutdown to flush pending writes.
func InitLogger(cfg *Config, opts ...zap.Option) (*zap.Logger, *ZapProperties, func(), error) {
	cfg.Initialize()

	var outputs []zapcore.WriteSyncer
	if len(cfg.File.Filename) > 0 {
		lg, err := initFileLog(&cfg.File)
		if err != nil {
			return nil, nil, nil, err
		}
		outputs = append(outputs, zapcore.AddSync(lg))
	}
	if cfg.Stdout {
		stdOut, _, err := zap.Open([]string{"stdout"}...)
		if err != nil {
			return nil, nil, nil, err
		}
		outputs = append(outputs, stdOut)
	}

	// Build with debug level internally; actual filtering done by AtomicLevel
	debugCfg := *cfg
	debugCfg.Level = "debug"
	outputsWriter := zap.CombineWriteSyncers(outputs...)

	debugL, props, cleanup, err := InitLoggerWithWriteSyncer(&debugCfg, outputsWriter, opts...)
	if err != nil {
		return nil, nil, nil, err
	}

	level := zapcore.DebugLevel
	parsedLevel := cfg.Level
	if strings.EqualFold(parsedLevel, "trace") {
		parsedLevel = "debug"
	}
	if err := level.UnmarshalText([]byte(parsedLevel)); err != nil {
		return nil, nil, cleanup, err
	}
	props.Level.SetLevel(level)

	return debugL, props, cleanup, nil
}

// InitTestLogger initializes a logger for unit tests.
func InitTestLogger(t zaptest.TestingT, cfg *Config, opts ...zap.Option) (*zap.Logger, *ZapProperties, func(), error) {
	writer := NewTestingWriter(t)
	zapOptions := []zap.Option{
		// Send zap errors to the same writer and mark the test as failed if
		// that happens.
		zap.ErrorOutput(writer.WithMarkFailed(true)),
	}
	opts = append(zapOptions, opts...)
	return InitLoggerWithWriteSyncer(cfg, writer, opts...)
}

// InitLoggerWithWriteSyncer initializes a zap logger with specified write syncer.
// The returned cleanup function should be called on shutdown.
func InitLoggerWithWriteSyncer(cfg *Config, output zapcore.WriteSyncer, opts ...zap.Option) (*zap.Logger, *ZapProperties, func(), error) {
	cfg.Initialize()

	level := zap.NewAtomicLevel()
	err := level.UnmarshalText([]byte(cfg.Level))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("InitLoggerWithWriteSyncer UnmarshalText cfg.Level err:%w", err)
	}

	var core zapcore.Core
	var cleanup func()
	if cfg.AsyncWriteEnable {
		asyncCore := NewAsyncTextIOCore(cfg, output, level)
		core = asyncCore
		cleanup = asyncCore.Stop
	} else {
		core = NewTextCore(NewTextEncoderByConfig(cfg), output, level)
		cleanup = func() {}
	}

	opts = append(cfg.BuildOptions(output), opts...)
	lg := zap.New(core, opts...)
	props := &ZapProperties{
		Core:   core,
		Syncer: output,
		Level:  level,
	}
	return lg, props, cleanup, nil
}

// initFileLog initializes file based logging options.
func initFileLog(cfg *FileLogConfig) (*lumberjack.Logger, error) {
	logPath := strings.Join([]string{cfg.RootPath, cfg.Filename}, string(filepath.Separator))
	if st, err := os.Stat(logPath); err == nil {
		if st.IsDir() {
			return nil, errors.New("can't use directory as log file name")
		}
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = defaultLogMaxSize
	}

	// use lumberjack to logrotate
	return &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxDays,
		LocalTime:  true,
	}, nil
}
