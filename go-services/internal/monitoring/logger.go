package monitoring

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"
)

type Logger struct {
	service string
	base    *log.Logger
}

func New(service string) Logger {
	if strings.TrimSpace(service) == "" {
		service = "go-services"
	}
	return Logger{
		service: service,
		base:    log.New(os.Stdout, "", 0),
	}
}

func (l Logger) Info(msg string, fields map[string]interface{}) {
	l.log("INFO", msg, fields)
}

func (l Logger) Warn(msg string, fields map[string]interface{}) {
	l.log("WARN", msg, fields)
}

func (l Logger) Error(msg string, err error, fields map[string]interface{}) {
	if fields == nil {
		fields = map[string]interface{}{}
	}
	if err != nil {
		fields["error"] = err.Error()
	}
	l.log("ERROR", msg, fields)
}

func (l Logger) log(level, msg string, fields map[string]interface{}) {
	line := fmt.Sprintf("ts=%s service=%s level=%s msg=%q", time.Now().UTC().Format(time.RFC3339Nano), l.service, level, msg)
	if len(fields) > 0 {
		keys := make([]string, 0, len(fields))
		for k := range fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			line += fmt.Sprintf(" %s=%v", k, fields[k])
		}
	}
	l.base.Println(line)
}
