package monitoring

import "log"

func Info(msg string) {
	log.Printf("INFO %s", msg)
}
