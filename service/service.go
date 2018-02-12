package service

import (
	"time"
	"sync"
	"log"
	"os"
	"bufio"
	"io"
	"encoding/json"
	"math/rand"

	"github.com/Shopify/sarama"
	conf "github.com/deathcore666/readWrite/config"
	"strconv"
	"io/ioutil"
	"strings"
	"encoding/csv"
)

func writeToKafka(dataStream chan []byte, interrupt chan os.Signal, wg *sync.WaitGroup){
	timer := time.NewTimer(time.Second * time.Duration(conf.MyConfig.Timer))
	start := time.Now()
	var (
		wgLocal                     sync.WaitGroup
		enqueued, successes, errors int
		err							error
	)

	config := sarama.NewConfig()
	sarama.MaxRequestSize = 1000000
	config.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer([]string{conf.MyConfig.KafkaAddress}, config)
	if err != nil {
		log.Println(err)
	}

	wgLocal.Add(1)
	go func() {
		defer wgLocal.Done()
		for range producer.Successes() {
			successes++
		}
	}()

	wgLocal.Add(1)
	go func() {
		defer wgLocal.Done()
		for err := range producer.Errors() {
			log.Println(err)
			errors++
		}
	}()

ProducerLoop:
	for {
		message := &sarama.ProducerMessage{Key: sarama.StringEncoder(time.Now().UnixNano()) ,Topic: conf.MyConfig.KafkaTopic, Value: sarama.StringEncoder(<-dataStream)}
		select {
		case producer.Input() <- message:
			enqueued++

		case <-timer.C:
			log.Println(strconv.Itoa(conf.MyConfig.Timer) + " second(s) timeout has passed")
			producer.AsyncClose() // Trigger a shutdown of the producer.
			err = ErrTimeout
			break ProducerLoop

		case <-interrupt:
			log.Println("system interrupt detected, finishing up...")
			producer.AsyncClose() // Trigger a shutdown of the producer.
			err = ErrInterrupt
			break ProducerLoop
		}
	}

	wgLocal.Wait()

	t := time.Now()
	elapsed := t.Sub(start)

	defer func() {
		wg.Done()
		log.Printf("kafka messages enqueued: %d; errors: %d\n", successes, errors)
		log.Println("producer exited; time elapsed: ", elapsed)
	}()

}

func readFile(dataStream chan []byte, wg *sync.WaitGroup) {
	file, err := os.Open("test.txt")
	if err != nil {
		log.Println(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			file.Close()
			readFile(dataStream, wg)
		}
		dataStream <- line
	}
}

func WriteRandomDataToFile() {
	start := time.Now()
	file, err := os.OpenFile(
		"test.txt",
		os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
		0666,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	for i := 0; i < 1000; i++ {
		_, err := file.Write(generateARecord())
		if err != nil {
			log.Fatal(err)
		}
		_, err = file.Write([]byte("\n"))
		if err != nil {
			log.Fatal(err)
		}
	}
	t := time.Now()
	elapsed := t.Sub(start)
	log.Println("Source text file generated; time elapsed:", elapsed)
}

func generateARecord() []byte {
	var (
		prefs, models, numbers []string
	)
	places := make(map[string]string)
	data := make(map[string]interface{})

	prefs = append(prefs,
		"99655",
		"99677",
		"99670",
		"99650",
	)
	models = append(models,
		"iPhone4",
		"iPhone5",
		"iPhone6",
		"iPhone7",
		"iPhone8",
		"iPhoneX",
		"Galaxys5",
		"Galaxys6",
		"Galaxys7",
		"Galaxys8",
	)

	file, err := ioutil.ReadFile("bishkekplaces.csv")
	if err != nil {
		log.Println(err)
	}

	r := csv.NewReader(strings.NewReader(string(file)))
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		numbers = append(numbers, record[1])
		places[record[0]] = record[1]
	}

	msisdn := numbers[rand.Intn(len(numbers))]
	res := strings.SplitAfterN(msisdn, "\n", 2)
	res[0] = strings.Replace(res[0], "\n", "", -1)
	randomTimeInt := rand.Int63n((time.Now().Unix()+3150000)-time.Now().Unix()) + time.Now().Unix()
	randomTime := time.Unix(randomTimeInt, 0)
	destNumber := 1000000 + rand.Intn(9999999-1000000)

	data["timestamp"] = randomTime.Format("2006-01-02 15:04:05")
	data["msi_sdn"] = "+" + prefs[rand.Intn(len(prefs))] + strconv.Itoa(destNumber)
	data["dest_number"] = res[0]
	data["duration"] = strconv.Itoa(rand.Intn(300)) + " s"
	data["phone_model"] = models[rand.Intn(len(models))]

	for i := 0; i < 200; i++ {
		num := rand.Int63()
		data["field"+string(i)] = num
	}

	exportData, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	return exportData
}
