package main

import (
	"github.com/deathcore666/readWrite/service"
	"runtime"
	"github.com/deathcore666/readWrite/config"
	"flag"
	"log"
)

func init() {
	runtime.GOMAXPROCS(4)
	config.LoadConfigs()
}

func main() {
	populateAFile := flag.Bool("populate", false,
		"add this flag if you do not yet have a source txt file")
	flag.Parse()
	//Run this once to populate a file with random data
	if *populateAFile {
		log.Println("Flag set to true")
		service.WriteRandomDataToFile()
	}

	service.RunService()
}
