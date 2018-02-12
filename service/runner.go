package service

import (
	"os"
	"time"
	"errors"
	"os/signal"
	"sync"
	"log"
)

type Runner struct {
	interrupt chan os.Signal
	complete  chan error
	timeout   <-chan time.Time
	tasks     []func()
}

var ErrTimeout = errors.New("received timeout")
var ErrInterrupt = errors.New("received interrupt")

func RunService() {
	myRunner := New()
	myRunner.Add(createTask())

	if err := myRunner.Start(); err != nil {
		switch err {
		case ErrInterrupt:
			log.Println("terminating due to system interrupt")
			os.Exit(2)
		}
	}

	log.Println("RunService process ended")
}

func createTask() func() {
	return func() {
		start := time.Now()
		log.Println("writing to Kafka...")

		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, os.Interrupt)

		done := make(chan interface{}, 1)
		messages := make(chan []byte)
		var wg sync.WaitGroup

		wg.Add(1)
		go writeToKafka(messages, interrupt, done, &wg)

		for {
			go readFile(messages, &wg)
		}

		wg.Wait()
		close(messages)
		t := time.Now()
		elapsed := t.Sub(start)
		log.Println("finished writing; time elapsed: ", elapsed)
	}
}

func New() *Runner {
	return &Runner{
		interrupt: make(chan os.Signal, 1),
		complete:  make(chan error),
	}
}

func (r *Runner) Add(tasks ...func()) {
	r.tasks = append(r.tasks, tasks...)
}

func (r *Runner) Start() error {
	signal.Notify(r.interrupt, os.Interrupt)

	go func() {
		r.complete <- r.run()
	}()

	select {
	case err := <-r.complete:
		return err
	}
}

func (r *Runner) run() error {
	for _, task := range r.tasks {
		task()
	}
	return nil
}