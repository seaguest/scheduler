package scheduler

import (
	"fmt"
	"sync"

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
	process func(interface{})
}

func New(maxRoutine int, chanSize int, process func(interface{})) *Scheduler {
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
	return s
}

// no duplicate request will be processed
func (s *Scheduler) NoDuplicate(noDup bool) {
	// TODO, size to be customized
	if noDup {
		s.cf = cuckoo.NewFilter(1000000)
	}
}

func (s *Scheduler) Start() {
	// start process
	for i := 0; i < s.maxRoutine; i++ {
		go s.processRequest()
	}
}

func (s *Scheduler) processRequest() {
	for {
		select {
		case req := <-s.reqChan:
			s.process(req)
			s.wg.Done()
		}
	}
}

// reqId : request id, to avoid duplicate request
func (s *Scheduler) Enqueue(params ...interface{}) error {
	if s.cf != nil && len(params) != 2 || s.cf == nil && len(params) != 1 {
		err := fmt.Errorf("invalid config")
		return err
	}

	var req interface{}
	if s.cf != nil {
		req = params[1]
		reqId := fmt.Sprint(params[0])
		if s.cf.Lookup([]byte(reqId)) {
			return nil
		}

		s.cf.InsertUnique([]byte(reqId))
	} else {
		req = params[0]
	}

	select {
	case s.reqChan <- req:
		s.wg.Add(1)
	}
	return nil
}

func (s *Scheduler) Wait() {
	s.wg.Wait()
}
