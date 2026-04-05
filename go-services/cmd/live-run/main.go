package main

import (
	"fmt"
	"polymarket-bot/go-services/internal/runner"
)

func main() {
	r := runner.NewService("live")
	fmt.Println(r.RunOnce())
}
