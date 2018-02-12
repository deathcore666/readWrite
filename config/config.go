package config

import (
	"io/ioutil"
	"fmt"
	"encoding/json"
)

type config struct {
	Timer        int
	KafkaAddress string
	KafkaTopic   string
	Limit        int
}

var MyConfig = config{1, "localhost:9092", "test3", 100000}

func LoadConfigs() {
	var localConfigs config

	dat, err := ioutil.ReadFile("config.json")
	if err != nil {
		fmt.Println("error:", err)
	}

	err = json.Unmarshal(dat, &localConfigs)
	if err != nil {
		fmt.Println("error:", err)
	}

	MyConfig = localConfigs
}
