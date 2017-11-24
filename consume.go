package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/jjeffery/stomp"
	"os/exec"
	"strings"
	"syscall"
)

var listen = flag.String("listen", "localhost:61613", "The host and port to listen on")
var limit = flag.Int("limit", 0, "Number of messages to consume before exit")
var queue = flag.String("queue", "", "Destination queue")
var command = flag.String("command", "", "Command to run on receive message")

var connOpts = []func(*stomp.Conn) error{
	stomp.ConnOpt.Login("system", "manager"),
	stomp.ConnOpt.UseStomp,
}

func main() {
	flag.Parse()

	if *limit <= 0 {
		fmt.Println("Invalid value for limit:", *limit)
		return
	}

	if len(*queue) == 0 {
		fmt.Println("Invalid queue provided:", *queue)
		return
	}

	fmt.Println("Starting queue worker...")

	conn, err := stomp.Dial("tcp", *listen, connOpts...)
	if err != nil {
		fmt.Println("Unable to connect to server", *listen, err.Error())
		return
	}

	defer conn.Disconnect()

	sub, err := conn.Subscribe(*queue, stomp.AckClientIndividual)
	if err != nil {
		fmt.Println("Unable to subscribe to", queue, err.Error())
		return
	}

	for i := 1; i <= *limit; i++ {
		msg := <-sub.C
		err := handleMessage(msg)

		if err != nil {
			fmt.Println("Received error when handling message:", err.Error())
			fmt.Println("Nacking message...")
			conn.Nack(msg)
			continue
		}

		fmt.Println("Acking message...")
		conn.Ack(msg)
	}

	fmt.Println("Message limit hit. Finishing up.")

	sub.Unsubscribe()
}

func handleMessage(msg *stomp.Message) error {
	args := strings.Split(*command, " ")
	args = append(args, string(msg.Body))

	cmd := exec.Command(args[0], args[1:]...)

	if err := cmd.Start(); err != nil {
		return errors.New(fmt.Sprint("Error starting command:", *command, err.Error()))
	}

	if err := cmd.Wait(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
				return errors.New(fmt.Sprintf(
					"Received exit code %d from command: %s",
					status.ExitStatus(),
					*command,
				))
			}
		} else {
			return errors.New(fmt.Sprintf("Error executing: %v", err))
		}
	}

	return nil
}
