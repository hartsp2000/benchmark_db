package main

import (
	"flag"
	"fmt"
	"github.com/hartsp2000/benchmark_db/arguments"
	"github.com/hartsp2000/benchmark_db/config"
	"github.com/hartsp2000/benchmark_db/db"
	"github.com/hartsp2000/benchmark_db/memory"
	"github.com/hartsp2000/benchmark_db/statistics"
	"github.com/hartsp2000/benchmark_db/version"
	"math/rand"
	"os"
	"sync"
	"time"
)

var configfile = "benchmark_db.conf"

// WARNING - In junkBytes Don't use ":" for random data generation.  ":" is reserved for redis functions

const junkBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()=+-;~/?<>[]{}_"
const sessBytes = "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789"

var JunkData [][]string
var JunkKey [][]string

var AvailData map[int]memory.Memory

var SessionName string

var WriteErrors, ReadErrors int
var IterRead, IterWrite [131073]time.Duration

func DisplayHelp() {
	fmt.Printf("Benchmark DB Tool - Version %s (%s)\n", version.VERSION, version.BUILDID)
	flag.PrintDefaults()
	fmt.Printf("\n")
	os.Exit(1)
}

func CommandLineArgs() arguments.Arguments {
	var help = flag.Bool("help", false, "Display list of commands")
	var nodatacheck = flag.Bool("ndc", false, "Skip data checking")
	var parallel = flag.Bool("p", false, "run in parallel mode (sequential test only!)")
	var loops = flag.Int("loops", 1, "Number of test loops (max 131072)")
	var iterations = flag.Int("iter", 1000, "Number of times to repeat the reads/writes (max 131072)")
	var mode = flag.String("mode", "rw", "r=read(for tps); w=write; rw=read/write")
	var dataBS = flag.Int("dbs", 1024, "Data Block byte size (for data writes)")
	var keyBS = flag.Int("kbs", 20, "Key Block byte size (for key size)")
	var db_type = flag.String("db", "cassandra", "DB Type Implementation name.")
	var tps = flag.Bool("tps", false, "TPS random read/write test. You must set duration value")
	var duration = flag.Int("dur", 0, "Duration (in minutes) to run TPS test.")
	var interval = flag.String("delay", "0", "Interval (delay time) to wait before opening additional "+
		"connections for TPS test. (eg: 1ns, 50ms, 1s)")
	var tpsWorkers = flag.Int("tw", 1, "TPS Test: Number of workers (threads) Must be 1 or equal "+
		"to the number of loops!!")
	var spatterns = flag.Bool("stored", false, "Use stored patterns from db in the config file")
	var sessovrd = flag.String("session", "", "Use an existing session name. (Don't create tables)")

	flag.Parse()

	if *help {
		DisplayHelp()
	}

	if !(*mode == "rw" || *mode == "w" || *mode == "r") {
		DisplayHelp()
	}

	if (*tpsWorkers != 1) && (*tpsWorkers != *loops) {

		fmt.Printf("Workers: %d    Loops: %d", *tpsWorkers, *loops)
		fmt.Printf("\nFatal: Incompatible worker and loop count.  Workers must be 1 or equal to the number of loops!\n\n")
		DisplayHelp()
	}

	if *tps && *duration < 1 {
		DisplayHelp()
	}

	arguments := arguments.Arguments{}
	arguments.Parallel = *parallel
	arguments.Loops = *loops
	arguments.Iterations = *iterations
	arguments.Mode = *mode
	arguments.DataBS = *dataBS
	arguments.KeyBS = *keyBS
	arguments.DB_Type = *db_type
	arguments.TPS = *tps
	arguments.Duration = *duration
	arguments.Interval = *interval
	arguments.TpsWorkers = *tpsWorkers
	arguments.NoDataCheck = *nodatacheck
	arguments.Spatterns = *spatterns
	arguments.Sessovrd = *sessovrd
	return arguments
}

func RandStringBytes(n int) string {
	rand.Seed(time.Now().UTC().UnixNano())
	b := make([]byte, n)
	for i := range b {
		b[i] = junkBytes[rand.Intn(len(junkBytes))]
	}
	return string(b)
}

func UniqStringBytes(l int, n int, st string) string {
	lnth := len(st)
	uniq := fmt.Sprintf("%d.%d.", l, n)
	ulen := len(uniq)
	nstr := fmt.Sprintf("%s%s", uniq, st[0:lnth-ulen])
	return nstr
}

func RandSessBytes(n int) string {
	rand.Seed(time.Now().UTC().UnixNano())
	b := make([]byte, n)
	for i := range b {
		b[i] = sessBytes[rand.Intn(len(sessBytes))]
	}
	return string(b)
}

func GenerateRandom(l int, n int, dbytes int, kbytes int) {
	fmt.Printf("Creating data pattern...")
	for loops := 0; loops < l; loops++ {
		fmt.Printf("%d.", loops+1)
		for iter := 0; iter < n; iter++ {
			JunkKey[loops][iter] = UniqStringBytes(loops, iter, RandStringBytes(kbytes))
			JunkData[loops][iter] = RandStringBytes(dbytes)
		}
	}
	fmt.Printf("  Success.\n")
}

func InitMem(loops int, iter int) {
	AvailData = make(map[int]memory.Memory)
	JunkData = make([][]string, loops)
	for i := range JunkData {
		JunkData[i] = make([]string, iter)
	}

	JunkKey = make([][]string, loops)
	for i := range JunkKey {
		JunkKey[i] = make([]string, iter)
	}
}

func main() {
	// LOAD THE CONFIG AND PROCESS COMMAND LINE ARGUMENTS
	var config config.Config = config.ReadConfig(configfile)
	var arguments arguments.Arguments = CommandLineArgs()

	// CREATE A SESSION NAME
	if i := len(arguments.Sessovrd); i == 0 {
		SessionName = RandSessBytes(10)
	} else {
		SessionName = arguments.Sessovrd
	}

	// SETUP THE DATABASE INTERFACE
	var idb db.Interface_DB
	var err error
	var read_stats *statistics.DurationSet
	var write_stats *statistics.DurationSet

	read_stats = &statistics.DurationSet{}
	write_stats = &statistics.DurationSet{}

	db.Init(config)

	// INITIALIZE THE MEMORY
	InitMem(arguments.Loops, arguments.Iterations)

	var wg sync.WaitGroup

	// SET THE COUNTERS
	WriteErrors = 0
	ReadErrors = 0

	// SHOW THE PROGRAM AND TEST INFO
	fmt.Printf("Benchmark DB Tool - Version %s (%s)\n", version.VERSION, version.BUILDID)
	fmt.Printf("Mode: %s, Iterations: %d, Key Size: %d bytes, Data Size: %d bytes Session-ID: %s\n", arguments.Mode,
		arguments.Iterations, arguments.KeyBS, arguments.DataBS, SessionName)

	if idb, err = db.Get(arguments.DB_Type); err != nil {
		fmt.Printf("Database %s doesn't exist\n", arguments.DB_Type)
		os.Exit(1)
	}

	if err = idb.Connect(config, arguments); err != nil {
		os.Exit(1)
	}

	// GENERATE SOME RANDOM DATA AND SAVE TO MEMORY OR LOAD EXISTING PATTERNS FROM DATABASE
	if arguments.Spatterns {
		JunkKey, JunkData, AvailData, _ = idb.ReadPatternData(SessionName, config, arguments)
	} else {
		GenerateRandom(arguments.Loops, arguments.Iterations, arguments.DataBS, arguments.KeyBS)
	}

	// CREATE TEST TABLES IF NOT USING AN EXISTING SESSION
	if i := len(arguments.Sessovrd); i == 0 {
		if err = idb.CreateTestTables(SessionName, arguments.Loops); err != nil {
			os.Exit(2)
		}
	}

	// DO THE TESTS
	if arguments.TPS {
		wg.Add(1)
		if arguments.Mode == "rw" {
			idb.TPSTestRW(SessionName, config, arguments, &wg, JunkData, JunkKey, AvailData, read_stats, write_stats)
		}
		if arguments.Mode == "w" {
			idb.TPSTestW(SessionName, config, arguments, &wg, JunkData, JunkKey, AvailData, read_stats, write_stats)
		}
		if arguments.Mode == "r" {
			idb.TPSTestR(SessionName, config, arguments, &wg, JunkData, JunkKey, AvailData, read_stats, write_stats)
		}
	} else {

		for loops := 0; loops < arguments.Loops; loops++ {

			if arguments.Parallel {
				time.Sleep(time.Millisecond * 200)
				wg.Add(1)
				go idb.TestCycle(SessionName, config, arguments, loops, &wg, JunkData, JunkKey, read_stats, write_stats)
			} else {
				wg.Add(1)
				idb.TestCycle(SessionName, config, arguments, loops, &wg, JunkData, JunkKey, read_stats, write_stats)
			}
		}
	}

	// WAIT FOR DATABASE ACTIVITY TO CEASE
	wg.Wait()

	// PRINT THE TIME RESULTS
	fmt.Printf("\nWrite Statistics (%d errors):\n    %s", idb.GetWriteErrors(), write_stats)
	fmt.Printf("Read Statistics (%d errors):\n    %s\n\n", idb.GetReadErrors(), read_stats)

}
