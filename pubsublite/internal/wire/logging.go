package wire

import "log"

const enableLogging = false

func Logf(format string, v ...interface{}) {
	if enableLogging {
		log.Printf(format, v...)
	}
}
