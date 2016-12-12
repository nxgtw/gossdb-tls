package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/matishsiao/gossdb/ssdb"
	"github.com/shirou/gopsutil/process"
)

var (
	size            = 300000
	cleanSize       = 200000
	conn            = 1
	cleanConn       = 1
	stop      int64 = 2
	Value     string
	queries   Queries
	mutex     = &sync.Mutex{}
	watchCPU  bool
)

type Queries struct {
	NXQuery int64                  `json:"nxquery"`
	Query   int64                  `json:"query"`
	Mins    int32                  `json:"mins"`
	NXMins  int32                  `json:"nxmins"`
	IDC     map[string]*IDCQueries `json:"idc"`
}

type IDCQueries struct {
	Mins   int32 `json:"mins"`
	NXMins int32 `json:"nxmins"`
}

func main() {
	SetUlimit(102000)
	//go getCPU()
	for i := 0; i < 1; i++ {
		batchTest(i)
		time.Sleep(10 * time.Second)
	}
}

func getCPU() {
	stats, _ := process.NewProcess(int32(os.Getpid()))
	for {
		cpu, _ := stats.Percent(1 * time.Second)
		log.Printf("CPU:%v\n", cpu)
		time.Sleep(1 * time.Second)
	}
}

func batchTest(id int) {
	wg := sync.WaitGroup{}
	write := true
	clear := false
	if clear {
		wg.Add(cleanConn)
		start := time.Now().Unix()
		log.Printf("----------------------------Batch Clear[%d]----------------------------", id)
		for i := 0; i < cleanConn; i++ {
			go batchClearTest(&wg, i)
		}
		wg.Wait()
		usingTime := time.Now().Unix() - start - stop
		log.Printf("----------------------------Batch Clear End[%d][%d]----------------------------", id, usingTime)
	}

	time.Sleep(time.Duration(stop) * time.Second)

	if write {
		queries.Mins = rand.Int31()
		queries.NXMins = rand.Int31()
		queries.Query = time.Now().Unix()
		queries.NXQuery = queries.Query / 10
		log.Printf("----------------------------Batch Add[%d]----------------------------", id)
		start := time.Now().Unix()
		wg.Add(conn)
		for i := 0; i < conn; i++ {
			go batchAddTest(&wg, i)
		}
		wg.Wait()
		usingTime := time.Now().Unix() - start - stop
		log.Printf("----------------------------Batch Add End[%d][%d]----------------------------", id, usingTime)
	}

}

func batchClearTest(wg *sync.WaitGroup, id int) {
	db, err := ssdb.Connect("104.155.216.67", 8080, "sa23891odi1@8hfn!0932aqiomc9AQjiHH")
	if err != nil {
		log.Println("Connect to Fail:", "104.155.216.67", id, err)
		return
	}
	defer wg.Done()
	Value := fmt.Sprintf("%d", time.Now().Unix())
	var cleanSplit [][]interface{}
	//start := 0
	//end := conn * size
	start := id * cleanSize
	end := (id + 1) * cleanSize
	for i := start; i < end; i++ {
		cleanSplit = append(cleanSplit, []interface{}{"hclear", fmt.Sprintf("BatchTest-%d", i)})
	}
	db.BatchSend(cleanSplit)
	time.Sleep(time.Duration(stop) * time.Second)
	log.Println(id, "-Clear-check start:", start, "end:", end)
	setList, err := db.Do("hlist", "BatchTest-", "C", -1)
	log.Println(id, "-Clear", len(setList))
	fail := ""
	if len(setList) != 1 {
		fail = fmt.Sprintf("Clear Fail:%d", len(setList))
	}
	log.Println("done", Value, fail)
	db.Close()
}

func batchAddTest(wg *sync.WaitGroup, id int) {
	db, err := ssdb.Connect("104.155.216.67", 8080, "sa23891odi1@8hfn!0932aqiomc9AQjiHH")
	if err != nil {
		log.Println("Connect to Fail:", "104.155.216.67", id, err)
		return
	}
	defer wg.Done()
	Value = fmt.Sprintf("%d", time.Now().Unix())

	datastr := ""
	jsonstr, err := json.Marshal(queries)
	if err != nil {
		datastr = `{"nxquery":0,"query":1586424,"mins":168,"nxmins":0,"idc":{"HK":{"mins":156,"nxmins":0}}}`
	} else {
		datastr = string(jsonstr)
	}
	var split [][]interface{}
	start := id * size
	end := (id + 1) * size
	for i := start; i < end; i++ {
		split = append(split, []interface{}{"hset", fmt.Sprintf("BatchTest-%d", i), Value, datastr})
	}
	db.BatchSend(split)
	time.Sleep(time.Duration(stop) * time.Second)
	log.Println(id, "-Add-check start")
	setList, err := db.Do("hlist", "BatchTest-", "C", -1)
	log.Println(id, "-Add", len(setList))
	//_, err = db.Exec()
	if err != nil {
		log.Println("Exec to Fail:", err)
	}
	fail := ""
	listSize := len(setList)
	if listSize < size {
		fail = fmt.Sprintf("origin:%d get:%d loss:%d", size, listSize, (size - listSize))
	}
	log.Println("finished:", Value, fail)
	//get data to verify
	rand.Seed(time.Now().Unix())
	addFail := false
	for i := 0; i < 100; i++ {
		idx := rand.Intn(size - 1)
		result, err := db.Do("hget", fmt.Sprintf("BatchTest-%d", idx), Value)
		if len(result) == 2 && result[1] != datastr {
			log.Printf("Check Add[%d]:%v Write:%v Error:%v not match.\n", idx, result, datastr, err)
			addFail = true
		} else if len(result) == 1 {
			log.Printf("Check Add Fail[%d]:%v Error:%v.\n", idx, result, err)
			addFail = true
		}

		time.Sleep(1 * time.Millisecond)
	}
	if !addFail {
		log.Println("Verification Succesful", Value)
	}
	db.Close()
}

func SetUlimit(number uint64) {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Println("[Error]: Getting Rlimit ", err)
	}
	rLimit.Max = number
	rLimit.Cur = number
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Println("[Error]: Setting Rlimit ", err)
	}
	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Println("[Error]: Getting Rlimit ", err)
	}
	log.Println("set file limit done:", rLimit)
}
