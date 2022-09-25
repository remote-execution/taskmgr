package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var dispatched int = 0
var consumer http.Server
var interruptChannel chan os.Signal
var errorChannel chan error
var taskChannel chan Task

type Response struct {
	name    string
	outcome string
}

func main() {
	// logging setup

	fmt.Println("setting up consumer")
	consumer = http.Server{
		Addr:         ":9000",
		Handler:      consumerRoute(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	fmt.Println("establishing error channel")
	errorChannel = make(chan error)

	fmt.Println("establishing task channel")
	taskChannel = make(chan Task)

	fmt.Println("establishing system interrupt channel")
	interruptChannel = make(chan os.Signal, 1)

	fmt.Println("establishing system interrupt notification")
	signal.Notify(interruptChannel, os.Interrupt, syscall.SIGTERM)

	fmt.Println("invoking goroutine consume")
	go consume(errorChannel)

	fmt.Println("called consume and now waiting")
	select {
	case err := <-errorChannel:
		fmt.Println("back in main thread with an error")
		fmt.Println(err)
	case sig := <-interruptChannel:
		fmt.Printf("%v signal type received, shutting down gracefully.\n", sig)
		os.Exit(0)
	}
}

func consumerRoute() http.Handler {

	fmt.Println("setting up new mux")
	consumerRouter := mux.NewRouter().StrictSlash(true)

	fmt.Println("establishing handler")
	consumerRouter.HandleFunc("/task", handle).Methods("POST")

	fmt.Println("returning consumer router")
	return consumerRouter
}

func handle(w http.ResponseWriter, r *http.Request) {

	fmt.Println("getting request body")
	reqBody, _ := ioutil.ReadAll(r.Body)

	var task Task
	fmt.Println("attempting to unmarshal request body to json for task")
	err := json.Unmarshal(reqBody, &task)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("request task name %s\n", task.TaskName)
	fmt.Printf("request task description %s\n", task.TaskDescription)

	fmt.Println("invoking goroutine dispatcher")
	go dispatcher(taskChannel)

	fmt.Println("writing task over task channel")
	taskChannel <- task

	fmt.Println("writing response header")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	outcome := Response{
		name:    "outcome",
		outcome: "success",
	}

	fmt.Println("attempting to marshal response")
	_, err = json.Marshal(outcome)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("exiting request handler")
}

func consume(errorChannel chan error) {

	fmt.Println("listening for requests")
	errorChannel <- consumer.ListenAndServe()
}

func dispatcher(taskChannel chan Task) {

	dispatched++
	dispatchedId := dispatched

	fmt.Printf("in dispatched %d\n", dispatched)
	task := <-taskChannel

	fmt.Printf("dispatcher has task %s\n", task.TaskName)

	if dispatched == 1 {
		fmt.Println("sleeping for 10 seconds")
		time.Sleep(10 * time.Second)
	} else if dispatched == 2 {
		fmt.Println("sleeping for 5 seconds")
		time.Sleep(5 * time.Second)
	} else if dispatched == 3 {
		fmt.Println("sleeping for 7 seconds")
		time.Sleep(7 * time.Second)
	} else {
		fmt.Println("sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
	}

	fmt.Printf("exiting dispatched %d\n", dispatchedId)
}
