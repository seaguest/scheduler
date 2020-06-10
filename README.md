# scheduler
A simple scheduler in Golang.


Usage:


``` 
package main

import (
	"time"

	"github.com/seaguest/scheduler"
)

func main() {
	start := time.Now()
	s1 := scheduler.New(1000, 10000, func(pr interface{}) {
		// TODO
	}, nil)
	s1.Start()

	for i := 0; i < 1000000; i++ {
		s1.Enqueue(i)
	}
	s1.Wait()
}

```
