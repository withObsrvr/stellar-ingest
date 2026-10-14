package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

func newAttemptID() string {
	var suffix [6]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return fmt.Sprintf("%s-%d", time.Now().UTC().Format("20060102T150405Z"), time.Now().UnixNano())
	}
	return fmt.Sprintf("%s-%s", time.Now().UTC().Format("20060102T150405Z"), hex.EncodeToString(suffix[:]))
}
