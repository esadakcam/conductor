package logger

import (
	"os"

	"github.com/sirupsen/logrus"
)

var log *logrus.Logger

func init() {
	log = logrus.New()
	log.SetOutput(os.Stdout)
	log.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "02-01-2006 15:04:05.000",
		FullTimestamp:   true,
	})
	log.SetLevel(logrus.InfoLevel)
}

// SetLevel sets the logging level
func SetLevel(level logrus.Level) {
	log.SetLevel(level)
}

// Debug logs a message at level Debug
func Debug(args ...interface{}) {
	log.Debug(args...)
}

// Debugf logs a formatted message at level Debug
func Debugf(format string, args ...interface{}) {
	log.Debugf(format, args...)
}

// Info logs a message at level Info
func Info(args ...interface{}) {
	log.Info(args...)
}

// Infof logs a formatted message at level Info
func Infof(format string, args ...interface{}) {
	log.Infof(format, args...)
}

// Warn logs a message at level Warn
func Warn(args ...interface{}) {
	log.Warn(args...)
}

// Warnf logs a formatted message at level Warn
func Warnf(format string, args ...interface{}) {
	log.Warnf(format, args...)
}

// Error logs a message at level Error
func Error(args ...interface{}) {
	log.Error(args...)
}

// Errorf logs a formatted message at level Error
func Errorf(format string, args ...interface{}) {
	log.Errorf(format, args...)
}

// Fatal logs a message at level Fatal then the process will exit with status 1
func Fatal(args ...interface{}) {
	log.Fatal(args...)
}

// Fatalf logs a formatted message at level Fatal then the process will exit with status 1
func Fatalf(format string, args ...interface{}) {
	log.Fatalf(format, args...)
}

// WithField creates an entry with a single field
func WithField(key string, value interface{}) *logrus.Entry {
	return log.WithField(key, value)
}

// WithFields creates an entry with multiple fields
func WithFields(fields logrus.Fields) *logrus.Entry {
	return log.WithFields(fields)
}

// GetLogger returns the underlying logrus logger instance
func GetLogger() *logrus.Logger {
	return log
}
