package main

import (
	"container/list"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Queue struct {
	queues  map[string]*list.List
	waiters map[string]*list.List
	mu      sync.RWMutex
}

func NewQueue() *Queue {
	return &Queue{
		queues:  make(map[string]*list.List),
		waiters: make(map[string]*list.List),
	}
}

func (q *Queue) Enqueue(key string, value string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if waiters, exists := q.waiters[key]; exists && waiters.Len() > 0 {
		element := waiters.Front()
		waiters.Remove(element)
		if waiters.Len() == 0 {
			delete(q.waiters, key)
		}
		element.Value.(chan string) <- value
		return
	}

	if _, exists := q.queues[key]; !exists {
		q.queues[key] = list.New()
	}
	q.queues[key].PushBack(value)
}

func (q *Queue) Dequeue(key string) (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	queue, exists := q.queues[key]
	if !exists || queue.Len() == 0 {
		return "", false
	}

	element := queue.Front()
	queue.Remove(element)
	if queue.Len() == 0 {
		delete(q.queues, key)
	}
	return element.Value.(string), true
}

func (q *Queue) removeWaiter(key string, ch chan string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if waiters, exists := q.waiters[key]; exists {
		for e := waiters.Front(); e != nil; e = e.Next() {
			if e.Value.(chan string) == ch {
				waiters.Remove(e)
				break
			}
		}
		if waiters.Len() == 0 {
			delete(q.waiters, key)
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "port required")
		os.Exit(1)
	}

	port := os.Args[1]
	addr := port
	if !strings.Contains(addr, ":") {
		addr = ":" + addr
	}

	queue := NewQueue()
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPut {
			http.Error(w, "", http.StatusMethodNotAllowed)
			return
		}

		queueName := strings.TrimPrefix(r.URL.Path, "/")
		if queueName == "" {
			http.Error(w, "", http.StatusBadRequest)
			return
		}

		if r.Method == http.MethodPut {
			value := r.URL.Query().Get("v")
			if value == "" {
				http.Error(w, "", http.StatusBadRequest)
				return
			}
			queue.Enqueue(queueName, value)
			w.WriteHeader(http.StatusOK) // 200, не 201!
			return
		}

		if msg, ok := queue.Dequeue(queueName); ok {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(msg))
			return
		}

		ch := make(chan string, 1)
		queue.mu.Lock()
		if _, exists := queue.waiters[queueName]; !exists {
			queue.waiters[queueName] = list.New()
		}
		queue.waiters[queueName].PushBack(ch)
		queue.mu.Unlock()

		var timeout *time.Duration
		if timeoutParam := r.URL.Query().Get("timeout"); timeoutParam != "" {
			if seconds, err := strconv.Atoi(timeoutParam); err == nil && seconds > 0 {
				d := time.Duration(seconds) * time.Second
				timeout = &d
			}
		}

		if timeout == nil {
			select {
			case msg := <-ch:
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(msg))
			case <-r.Context().Done():
				queue.removeWaiter(queueName, ch)
			}
		} else {
			select {
			case msg := <-ch:
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(msg))
			case <-time.After(*timeout):
				queue.removeWaiter(queueName, ch)
				http.Error(w, "", http.StatusNotFound) // 404!
			case <-r.Context().Done():
				queue.removeWaiter(queueName, ch)
			}
		}
	})

	fmt.Printf("Server running on http://localhost:%s\n", port)

	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
