package scheduler

import (
	"fmt"
	"sync"
	"time"

	"github.com/seiflotfy/cuckoofilter"
)

// a scheduler is composed by load function and process function
type Scheduler struct {
	// if NoDuplicate called, not nil
	cf *cuckoo.Filter

	// query channel
	reqChan chan interface{}

	// max routine
	maxRoutine int

	// max routine
	chanSize int

	wg sync.WaitGroup

	// query process function
	process func(interface{}) error

	// get id function
	idFunc func(interface{}) interface{}
}

func New(maxRoutine int, chanSize int, process func(interface{}) error, idFunc func(interface{}) interface{}) *Scheduler {
	s := &Scheduler{}
	if maxRoutine == 0 {
		s.maxRoutine = 10
	} else {
		s.maxRoutine = maxRoutine
	}

	if chanSize == 0 {
		s.chanSize = 100
	} else {
		s.chanSize = chanSize
	}

	s.reqChan = make(chan interface{}, s.chanSize)
	s.process = process
	if idFunc != nil {
		s.idFunc = idFunc
		s.cf = cuckoo.NewFilter(1000000)
	}
	return s
}

func (s *Scheduler) Start() {
	s.wg.Add(s.maxRoutine)
	// start process
	for i := 0; i < s.maxRoutine; i++ {
		go s.processRequest()
	}
}

func (s *Scheduler) processRequest() {
	for req := range s.reqChan {
		if err := s.process(req); err != nil {
			if s.idFunc != nil {
				id := s.idFunc(req)
				reqId := []byte(fmt.Sprint(id))
				s.cf.Delete(reqId)
			}
		}
	}
	s.wg.Done()
}

// first param is request, second is id
func (s *Scheduler) Enqueue(req interface{}) error {
	if s.idFunc != nil {
		id := s.idFunc(req)
		reqId := []byte(fmt.Sprint(id))
		if s.cf.Lookup(reqId) {
			return nil
		}
		s.cf.InsertUnique(reqId)
	}

	s.reqChan <- req
	return nil
}

func (s *Scheduler) Wait() {
	// check if no element in queue
	for {
		time.Sleep(time.Millisecond * 10)
		if len(s.reqChan) == 0 {
			break
		}
	}
	// close queue
	close(s.reqChan)
	s.wg.Wait()
}
