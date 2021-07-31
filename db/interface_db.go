package db

import (
	"errors"
	"github.com/hartsp2000/benchmark_db/arguments"
	"github.com/hartsp2000/benchmark_db/config"
	"github.com/hartsp2000/benchmark_db/db/cassandra"
	"github.com/hartsp2000/benchmark_db/db/hbase"
	"github.com/hartsp2000/benchmark_db/db/postgres"
	"github.com/hartsp2000/benchmark_db/db/redis"
	"github.com/hartsp2000/benchmark_db/memory"
	"github.com/hartsp2000/benchmark_db/statistics"
	"sync"
)

type Interface_DB interface {
	Connect(config config.Config, arguments arguments.Arguments) (err error)
	CreateTestTables(SessionName string, nb_tables int) (err error)
	GetReadErrors() int
	GetWriteErrors() int

	ReadPatternData(SessionName string, config config.Config, arguments arguments.Arguments) (JunkKey [][]string, JunkData [][]string, AvailData map[int]memory.Memory, err error)

	TPSTestR(SessionName string, config config.Config, arguments arguments.Arguments, wg *sync.WaitGroup, JunkData [][]string, JunkKey [][]string, AvailData map[int]memory.Memory, read_stats *statistics.DurationSet, write_stats *statistics.DurationSet) (err error)

	TPSTestW(SessionName string, config config.Config, arguments arguments.Arguments, wg *sync.WaitGroup, JunkData [][]string, JunkKey [][]string, AvailData map[int]memory.Memory, read_stats *statistics.DurationSet, write_stats *statistics.DurationSet) (err error)

	TPSTestRW(SessionName string, config config.Config, arguments arguments.Arguments, wg *sync.WaitGroup, JunkData [][]string, JunkKey [][]string, AvailData map[int]memory.Memory, read_stats *statistics.DurationSet, write_stats *statistics.DurationSet) (err error)

	TestCycle(SessionName string, config config.Config, arguments arguments.Arguments, currentLoop int, wg *sync.WaitGroup, JunkData [][]string, JunkKey [][]string, read_stats *statistics.DurationSet, write_stats *statistics.DurationSet) (err error)
}

var (
	name2db map[string]Interface_DB
)

func init() {
	name2db = make(map[string]Interface_DB)
}

func Init(config config.Config) {
	cass := cassandra.New()
	hbase := hbase.New()
	redis := redis.New()
	postgres := postgres.New()

	name2db["cassandra"] = cass
	name2db["hbase"] = hbase
	name2db["redis"] = redis
	name2db["postgres"] = postgres
}

func Get(db_name string) (db Interface_DB, err error) {
	var ok bool

	db, ok = name2db[db_name]

	if !ok {
		// DB doesn't exist
		return nil, errors.New("Invalid DB Name")
	}

	return db, nil
}
