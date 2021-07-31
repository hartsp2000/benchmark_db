package redis

import (
	"fmt"
	"github.com/hartsp2000/benchmark_db/arguments"
	"github.com/hartsp2000/benchmark_db/config"
	"github.com/hartsp2000/benchmark_db/memory"
	"github.com/hartsp2000/benchmark_db/statistics"
	"github.com/hartsp2000/benchmark_db/timeparse"
	"gopkg.in/redis.v5"
	"math/rand"
	"os"
	"sync"
	"time"
)

var mux sync.Mutex

type RedisDB struct {
	session     *redis.ClusterClient
	WriteErrors int
	ReadErrors  int
}

type Results struct {
	ops         int64
	duration    time.Duration
	readErrors  int
	writeErrors int
	data        string
}

func New() *RedisDB {
	var tmp RedisDB = RedisDB{}
	return &tmp
}

func (db *RedisDB) Connect(config config.Config, arguments arguments.Arguments) (err error) {
	var connectstr []string
	connectstr = config.Clusternodes

	db.session = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    connectstr,
		Password: config.Password,
	})

	_, err = db.session.Ping().Result()

	if err != nil {
		return err
	}

	fmt.Printf("Connection to database was successful!\n")

	return nil

}

func (db *RedisDB) CreateTestTables(SessionName string, nb_tables int) (err error) {
	fmt.Printf("%s: Creation of %d tables skipped for Redis database type.\n", SessionName, nb_tables)
	return nil
}

func (db *RedisDB) ReadPatternData(SessionName string, config config.Config, arguments arguments.Arguments) (JunkKey [][]string, JunkData [][]string, AvailData map[int]memory.Memory, err error) {

	panic("Read Pattern Data not available in Redis.\n")
}

func (db *RedisDB) GetReadErrors() int {
	return db.ReadErrors
}

func (db *RedisDB) GetWriteErrors() int {
	return db.WriteErrors
}

func writeTestData(SessionName string, db *RedisDB, loop int, iter int, key string, data string) (err error) {
	var keyField string

	keyField = fmt.Sprintf("%s:%d:%d:%s:", SessionName, loop, iter, key)

	err = db.session.Set(keyField, data, 0).Err()

	if err != nil {
		return err
	}

	return nil
}

func WriteSequentialTestData(SessionName string, ch chan Results, db *RedisDB, delay time.Duration, duration int, loopstart int, loopend int, iterstart int, iterend int, JunkData [][]string, JunkKey [][]string) {
	defer close(ch)
	var ops int64 = 0
	errors := 0
	startTest := time.Now()
	start := time.Now().Unix()
	stop := start + (int64(duration) * 60)
	res := new(Results)

	for time.Now().Unix() < stop {
		for loop := loopstart; loop < loopend; loop++ {
			for iter := iterstart; iter < iterend; iter++ {
				time.Sleep(delay)
				if err := writeTestData(SessionName, db, loop, iter, JunkKey[loop][iter], JunkData[loop][iter]); err != nil {
					errors++
				}
				ops++
			}
		}
	}
	res.ops = ops
	res.duration = time.Since(startTest)
	res.writeErrors = errors
	ch <- *res
}

func readTestData(SessionName string, db *RedisDB, loop int, iter int, key string) (data string, err error) {
	var keyField string

	keyField = fmt.Sprintf("%s:%d:%d:%s:", SessionName, loop, iter, key)

	data, err = db.session.Get(keyField).Result()

	if err != nil {
		return "", err
	}

	return data, nil
}

func readOrWriteTestData(SessionName string, ch chan Results, db *RedisDB, delay time.Duration, duration int, loop int, iter int, JunkData [][]string, JunkKey [][]string, AvailData map[int]memory.Memory, nodatacheck bool) {
	defer close(ch)
	var ops int64 = 0
	readerr := 0
	writeerr := 0
	startTest := time.Now()
	start := time.Now().Unix()
	stop := start + (int64(duration) * 60)
	res := new(Results)

	for time.Now().Unix() < stop {
		readWrite := rand.Intn(2) // DETERMINE IF THIS IS READ OR WRITE
		if readWrite == 0 {
			rX := loop
			rY := rand.Intn(iter)
			time.Sleep(delay)
			if err := writeTestData(SessionName, db, rX, rY, JunkKey[rX][rY], JunkData[rX][rY]); err != nil {
				writeerr++
			}
			mux.Lock()
			AvailData[len(AvailData)] = memory.Memory{rX, rY}
			mux.Unlock()
			ops++
			continue
		}
		if len(AvailData) < 1 { // LOOP AGAIN IF NO DATA WRITTEN
			continue
		}
		mux.Lock()
		dataPoint := AvailData[rand.Intn(len(AvailData))]
		mux.Unlock()
		randLoop := dataPoint.Loop
		randIter := dataPoint.Iter
		mux.Lock()
		id := JunkKey[randLoop][randIter]
		mux.Unlock()

		data, err := readTestData(SessionName, db, randLoop, randIter, id)
		if err != nil {
			readerr++
		}
		mux.Lock()
		if err := checkData(JunkData[randLoop][randIter], data, nodatacheck); err != nil {
			readerr++
		}
		mux.Unlock()
		ops++
		continue
	}
	mux.Lock()
	res.ops = ops
	res.duration = time.Since(startTest)
	res.readErrors = readerr
	res.writeErrors = writeerr
	mux.Unlock()
	ch <- *res
}

func ReadRandomTestData(SessionName string, ch chan Results, db *RedisDB, delay time.Duration, duration int, loop int, iter int, JunkData [][]string, JunkKey [][]string, nodatacheck bool) {
	defer close(ch)
	var ops int64 = 0
	errors := 0
	startTest := time.Now()
	start := time.Now().Unix()
	stop := start + (int64(duration) * 60)
	res := new(Results)

	for time.Now().Unix() < stop {
		readIter := rand.Intn(iter)
		time.Sleep(delay)
		data, err := readTestData(SessionName, db, loop, readIter, JunkKey[loop][readIter])
		if err != nil {
			errors++
		}
		if err := checkData(JunkData[loop][readIter], data, nodatacheck); err != nil {
			errors++
		}
		ops++
	}
	res.ops = ops
	res.duration = time.Since(startTest)
	res.readErrors = errors
	ch <- *res
}

func showStats(read int, write int, ops int64, duration int64, tps int64) {
	fmt.Printf("\n\nRead Errors: %d\n", read)
	fmt.Printf("Write Errors: %d\n", write)
	fmt.Printf("Total Operations: %d\n", ops)
	fmt.Printf("Time Elapsed: %d seconds\n", duration)
	fmt.Printf("TPS Rate: %d\n\n", tps)
	return
}

func checkData(ctrl string, tst string, nodatacheck bool) (err error) {
	if nodatacheck {
		return nil
	}

	if ctrl == tst {
		return nil
	}

	err = fmt.Errorf("!! Data mismatch !!\nExpected: %s\nReceived:%s", ctrl, tst)
	return err
}

func (db *RedisDB) TPSTestR(SessionName string, config config.Config, arguments arguments.Arguments, wg *sync.WaitGroup, JunkData [][]string, JunkKey [][]string, AvailData map[int]memory.Memory, read_stats *statistics.DurationSet, write_stats *statistics.DurationSet) (err error) {
	var ops int64
	var elapsedTime int64
	var workers map[int]chan Results
	var TPS int64

	defer wg.Done()

	if i := len(arguments.Sessovrd); i == 0 {
		for loop := 0; loop < arguments.Loops; loop++ {
			fmt.Printf("Loop %d: Preparing database with sample records...", loop+1)
			for iter := 0; iter < arguments.Iterations; iter++ {
				StartWrite := time.Now()
				if err := writeTestData(SessionName, db, loop, iter, JunkKey[loop][iter], JunkData[loop][iter]); err != nil {
					fmt.Printf("\nLoop: %d, Iteration: %d --  %s\n", loop+1, iter+1, err)
					db.WriteErrors++
				}
				StopWrite := time.Since(StartWrite)
				write_stats.Add(StopWrite)
				AvailData[len(AvailData)] = memory.Memory{loop, iter}
			}
			fmt.Printf("Complete.\n")
		}
	}

	rand.Seed(time.Now().UTC().UnixNano())

	ops = 0
	StartTest := time.Now().Unix()
	intervalDuration := timeparse.ParseDuration(arguments.Interval)
	workers = make(map[int]chan Results)

	fmt.Printf("Starting %d workers...", arguments.TpsWorkers)

	if arguments.TpsWorkers == 1 {
		// SINGLE WORKER
		for loop := 0; loop < arguments.Loops; loop++ {
			fmt.Printf("Loop %d...", loop+1)
			workers[loop] = make(chan Results, 1)
			go ReadRandomTestData(SessionName, workers[loop], db, intervalDuration, arguments.Duration, loop, arguments.Iterations, JunkData, JunkKey, arguments.NoDataCheck)
			res := <-workers[loop]
			ops = ops + res.ops
			db.ReadErrors = db.ReadErrors + res.readErrors
		}
	} else {
		// FOR MULTIPLE WORKERS
		for channel := 0; channel < arguments.TpsWorkers; channel++ {
			workers[channel] = make(chan Results, arguments.TpsWorkers)
			go ReadRandomTestData(SessionName, workers[channel], db, intervalDuration, arguments.Duration, channel, arguments.Iterations, JunkData, JunkKey, arguments.NoDataCheck)
		}

		fmt.Printf("Started!  Test is running...")

		for results := range workers {
			res := <-workers[results]
			ops = ops + res.ops
			db.ReadErrors = db.ReadErrors + res.readErrors
		}
	}

	elapsedTime = time.Now().Unix() - StartTest
	if elapsedTime > 0 {
		TPS = ops / elapsedTime
	}

	showStats(db.ReadErrors, db.WriteErrors, ops, elapsedTime, TPS)
	os.Exit(0)
	return nil
}

func (db *RedisDB) TPSTestW(SessionName string, config config.Config, arguments arguments.Arguments, wg *sync.WaitGroup, JunkData [][]string, JunkKey [][]string, AvailData map[int]memory.Memory, read_stats *statistics.DurationSet, write_stats *statistics.DurationSet) (err error) {
	var ops int64
	var elapsedTime int64
	var TPS int64
	var workers map[int]chan Results

	defer wg.Done()

	rand.Seed(time.Now().UTC().UnixNano())

	ops = 0
	StartTest := time.Now().Unix()
	intervalDuration := timeparse.ParseDuration(arguments.Interval)
	workers = make(map[int]chan Results)

	fmt.Printf("Starting %d workers...", arguments.TpsWorkers)

	if arguments.TpsWorkers == 1 {
		// SINGLE WORKER
		for loop := 0; loop < arguments.Loops; loop++ {
			fmt.Printf("Loop %d...", loop+1)
			workers[loop] = make(chan Results, 1)
			go WriteSequentialTestData(SessionName, workers[loop], db, intervalDuration, arguments.Duration, loop, loop+1,
				0, arguments.Iterations, JunkData, JunkKey)
			res := <-workers[loop]
			ops = ops + res.ops
			db.WriteErrors = db.WriteErrors + res.writeErrors
		}
	} else {
		// FOR MULTIPLE WORKERS
		for channel := 0; channel < arguments.TpsWorkers; channel++ {
			workers[channel] = make(chan Results, arguments.TpsWorkers)
			go WriteSequentialTestData(SessionName, workers[channel], db, intervalDuration, arguments.Duration, channel, channel+1,
				0, arguments.Iterations, JunkData, JunkKey)
		}

		fmt.Printf("Started!  Test is running...")

		for results := range workers {
			res := <-workers[results]
			ops = ops + res.ops
			db.WriteErrors = db.WriteErrors + res.writeErrors
		}
	}

	elapsedTime = time.Now().Unix() - StartTest
	if elapsedTime > 0 {
		TPS = ops / elapsedTime
	}

	showStats(db.ReadErrors, db.WriteErrors, ops, elapsedTime, TPS)
	os.Exit(0)
	return nil
}

func (db *RedisDB) TPSTestRW(SessionName string, config config.Config, arguments arguments.Arguments, wg *sync.WaitGroup, JunkData [][]string, JunkKey [][]string, AvailData map[int]memory.Memory, read_stats *statistics.DurationSet, write_stats *statistics.DurationSet) (err error) {
	var ops int64
	var elapsedTime int64
	var TPS int64
	var workers map[int]chan Results

	defer wg.Done()

	rand.Seed(time.Now().UTC().UnixNano())

	ops = 0
	StartTest := time.Now().Unix()
	intervalDuration := timeparse.ParseDuration(arguments.Interval)
	workers = make(map[int]chan Results)

	fmt.Printf("Starting %d workers...", arguments.TpsWorkers)
	if arguments.TpsWorkers == 1 {
		// SINGLE WORKER
		for loop := 0; loop < arguments.Loops; loop++ {
			fmt.Printf("Loop %d...", loop+1)
			workers[loop] = make(chan Results, 1)
			go readOrWriteTestData(SessionName, workers[loop], db, intervalDuration, arguments.Duration, loop, arguments.Iterations, JunkData, JunkKey, AvailData, arguments.NoDataCheck)
			res := <-workers[loop]
			ops = ops + res.ops
			db.WriteErrors = db.WriteErrors + res.writeErrors
			db.ReadErrors = db.ReadErrors + res.readErrors
		}
	} else {
		// FOR MULTIPLE WORKERS
		for channel := 0; channel < arguments.TpsWorkers; channel++ {
			workers[channel] = make(chan Results, arguments.TpsWorkers)
			go readOrWriteTestData(SessionName, workers[channel], db, intervalDuration, arguments.Duration, channel, arguments.Iterations, JunkData, JunkKey, AvailData, arguments.NoDataCheck)
		}

		fmt.Printf("Started!  Test is running...")

		for results := range workers {
			res := <-workers[results]
			ops = ops + res.ops
			db.WriteErrors = db.WriteErrors + res.writeErrors
			db.ReadErrors = db.ReadErrors + res.readErrors
		}

	}

	elapsedTime = time.Now().Unix() - StartTest
	if elapsedTime > 0 {
		TPS = ops / elapsedTime
	}

	showStats(db.ReadErrors, db.WriteErrors, ops, elapsedTime, TPS)
	os.Exit(0)
	return nil
}

func (db *RedisDB) TestCycle(SessionName string, config config.Config, arguments arguments.Arguments, currentLoop int, wg *sync.WaitGroup, JunkData [][]string, JunkKey [][]string, read_stats *statistics.DurationSet, write_stats *statistics.DurationSet) (err error) {
	defer wg.Done()

	fmt.Printf("Loop %d: Beginning Write Test...\n", currentLoop+1)
	StartLoop := time.Now()
	for iter := 0; iter < arguments.Iterations; iter++ {
		StartWrite := time.Now()
		if err := writeTestData(SessionName, db, currentLoop, iter, JunkKey[currentLoop][iter], JunkData[currentLoop][iter]); err != nil {
			fmt.Printf("Loop: %d, Iteration: %d --  %s\n", currentLoop+1, iter+1, err)
			db.WriteErrors++
		}
		StopWrite := time.Since(StartWrite)
		write_stats.Add(StopWrite)
	}
	StopLoop := time.Since(StartLoop)

	fmt.Printf("Loop %d: Write Test Completed. (%s elapsed)\n", currentLoop+1, StopLoop)

	if arguments.Mode == "w" {
		return
	}

	// READ DATA AND VERIFY
	fmt.Printf("Loop %d: Beginning Read and Verify...\n", currentLoop+1)
	StartLoop = time.Now()
	for iter := 0; iter < arguments.Iterations; iter++ {
		StartRead := time.Now()
		data, err := readTestData(SessionName, db, currentLoop, iter, JunkKey[currentLoop][iter])
		if err != nil {
			fmt.Printf("Loop %d, Iteration: %d -- %s\n", currentLoop+1, iter+1, err)
			db.ReadErrors++
		}
		StopRead := time.Since(StartRead)
		read_stats.Add(StopRead)

		if err := checkData(JunkData[currentLoop][iter], data, arguments.NoDataCheck); err != nil {
			fmt.Printf("Loop %d, Iteration: %d\n%s", currentLoop+1, iter+1, err)
			db.ReadErrors++
		}
	}
	StopLoop = time.Since(StartLoop)
	fmt.Printf("Loop %d: Read Test Completed. (%s elapsed)\n", currentLoop+1, StopLoop)

	return nil
}
