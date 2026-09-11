package main

import (
	"github.com/abhiraj-ku/geki/internals/config"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		panic("env variable init failed")
	}
	cfg := config.Load()
	_ = cfg
}
