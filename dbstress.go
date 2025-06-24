package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	randv2 "math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/dustin/go-humanize"
)

type DB struct {
	*log.Logger
	session     *gocql.Session
	servers     []string
	concurrency int
	partitions  int
	valueLen    int
	opsPerIter  int
	delay       time.Duration
	rows        int64
	bwLog       io.WriteCloser
}

func main() {
	// Set up flags
	pflag.StringSlice("servers", []string{"localhost"}, "Comma-separated list of Cassandra server IPs or hostnames")
	pflag.String("keyspace", "dbstress", "Cassandra keyspace")
	pflag.String("username", "cassandra", "Cassandra username")
	pflag.String("password", "cassandra", "Cassandra password")
	pflag.Int("port", 9042, "Cassandra port")
	pflag.Int("concurrency", 100, "Number of concurrent goroutines")
	pflag.Int("partitions", 100, "Number of database partitions")
	pflag.Int("ops", 10000, "Number of database operations per iteration")
	pflag.String("valuelen", "1024", "Size of each database value (e.g. '1KiB')")
	pflag.String("delay", "0s", "Delay between tests (e.g. '500ms', '1s')")
	pflag.Parse()

	// Bind flags to viper
	viper.BindPFlags(pflag.CommandLine)

	// Enable config file support
	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("json")   // or "yaml"
	viper.AddConfigPath(".")      // look in current directory

	// Read in the config file if present
	if err := viper.ReadInConfig(); err == nil {
		fmt.Printf("Using config file: %s\n", viper.ConfigFileUsed())
	}

	servers := viper.GetStringSlice("servers")

	delay, err := time.ParseDuration(viper.GetString("delay"))
	if err != nil {
		log.Fatalf("Invalid delay duration: %s", err)
	}

	// Connect to Cassandra
	cluster := gocql.NewCluster(servers...)
	cluster.Port = viper.GetInt("port")
	cluster.Keyspace = viper.GetString("keyspace")
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = 5 * time.Second
	cluster.DisableInitialHostLookup = true
	cluster.IgnorePeerAddr = true
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: viper.GetString("username"),
		Password: viper.GetString("password"),
	}

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Unable to connect to cluster: %v", err)
	}
	defer session.Close()

	bwLog, err := os.OpenFile("log.csv", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0660)

	if err != nil {
		log.Fatalf("Failed opening log file: %s\n", err)
	} else {
		defer bwLog.Close()
	}

	db := &DB{
		Logger:      log.Default(),
		session:     session,
		servers:     servers,
		concurrency: viper.GetInt("concurrency"),
		partitions:  viper.GetInt("partitions"),
		valueLen:    int(viper.GetSizeInBytes("valuelen")),
		opsPerIter:  viper.GetInt("ops"),
		delay:       delay,
		rows:        0,
		bwLog:       bwLog,
	}

	err = db.PreStart()

	if err != nil {
		log.Fatalf("Unable to init rows: %s\n", err)
	}

	fmt.Printf("==== dbstress starting ====\n")
	fmt.Printf(" servers:          %v\n", cluster.Hosts)
	fmt.Printf(" starting rows:    %s\n", humanize.Comma(db.rows))
	fmt.Printf(" concurrency:      %d\n", db.concurrency)
	fmt.Printf(" partitions:       %d\n", db.partitions)
	fmt.Printf(" value len:        %d\n", db.valueLen)
	fmt.Printf(" inter-test delay: %s\n", db.delay)

	// db.DumpPartitionSizes()
	// return

	db.Run()
}

func (db *DB) Run() {
	// Register for control-C
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	for {
		select {
		case <-interrupt:
			fmt.Printf("Control-C; exiting with %d rows.\n", db.rows)
			return
		default:
			irate, err := db.Insert()

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			db.InterTestDelay()

			srate := 0
			srate2 := 0

			// Only do get/scan tests every 100K ops
			if db.rows%100000 == 0 {
				srate, err = db.SelectOne()

				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}

				db.InterTestDelay()

				srate2, err = db.SelectMany()

				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}

				db.InterTestDelay()
			}

			if srate > 0 {
				_, _ = fmt.Fprintf(db.bwLog, "%d, %d, %d, %d\n", db.rows, irate, srate, srate2)
				fmt.Printf(" - %s, insert %d rows/sec, select %d rows/sec, many %d rows/sec\n", humanize.SI(float64(db.rows), "rows"), irate, srate, srate2)
			} else {
				_, _ = fmt.Fprintf(db.bwLog, "%d, %d, NA, NA\n", db.rows, irate)
				fmt.Printf(" - %s, insert %d rows/sec\n", humanize.SI(float64(db.rows), "rows"), irate)
			}
		}
	}
}

func (db *DB) PreStart() error {
	// Check if the keyspace exists by attempting to describe a known table
	var tables int
	err := db.session.Query("SELECT COUNT(table_name) FROM system_schema.tables WHERE keyspace_name = ?", "dbstress").Scan(&tables)
	if err != nil {
		return fmt.Errorf("failed get table count: %w", err)
	}

	if tables != 2 {
		if err := db.LoadSchema(); err != nil {
			return fmt.Errorf("failed to load schema: %w", err)
		}
	}

	// Load db.rows from the rows table
	var rows int64
	err = db.session.Query("SELECT rows FROM rows WHERE id = ?", 0).Scan(&rows)
	if err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return fmt.Errorf("no entry for row count (should never happen)")
		} else {
			return fmt.Errorf("error querying row count: %w", err)
		}
	}
	db.rows = rows

	return nil
}

func (db *DB) LoadSchema() error {
	fmt.Printf("Loading schema\n")

	for _, statement := range schema {
		statement = strings.TrimSpace(statement)

		if len(statement) > 0 {
			err := db.session.Query(statement).Exec()

			if err != nil {
				return fmt.Errorf("error loading schema at '%s': %s", statement, err)
			}
		}
	}

	return nil
}

var insertRetryPolicy = NewRetryPolicy(3, 10*time.Millisecond, 1.2, func(e error) error { return nil })

func (db *DB) Insert() (int, error) {
	entries := db.PsvGenerator(db.rows, true)
	sem := make(chan struct{}, db.concurrency)
	var wg sync.WaitGroup

	start := time.Now()

	for psv := range entries {
		wg.Add(1)
		sem <- struct{}{} // acquire
		go func(psv *PSV) {
			// Retry here because I'm very occasionally seeing long latency spikes that cause insert to time out
			err := insertRetryPolicy.Retry(func() error {
				return db.session.Query(`INSERT INTO kv (p, s, v) VALUES (?, ?, ?)`,
					psv.p, psv.s, psv.v).Consistency(gocql.LocalQuorum).Exec()
			})

			if err != nil {
				fmt.Printf("error inserting entry: %s", err)
				os.Exit(1) // FIXME: too lazy to do this better at the moment
			}

			<-sem // release
			wg.Done()
		}(psv)
	}

	wg.Wait()
	elapsed := time.Since(start).Seconds()

	// Update the row count
	db.rows += int64(db.opsPerIter)
	err := db.session.Query(`UPDATE rows SET rows = ? WHERE id = ?`, db.rows, 0).Consistency(gocql.LocalQuorum).Exec()

	if err != nil {
		return 0, fmt.Errorf("error updating row count: %s", err)
	}

	return int(float64(db.opsPerIter) / elapsed), nil
}

// SelectOne retrieves db.opsPerIter individual rows from the database. It
// starts at a random position lower than db.rows, rounded down to the next
// lowest db.opsPerIter. It then uses PsvGenerator to get a list of
// partition/sort keys which should already exist in the database.
func (db *DB) SelectOne() (int, error) {
	// Determine starting point rounded down to the nearest opsPerIter
	maxStart := db.rows - int64(db.opsPerIter)
	base := (time.Now().UnixNano() % maxStart) / int64(db.opsPerIter) * int64(db.opsPerIter)
	entries := db.PsvGenerator(base, false)
	sem := make(chan struct{}, db.concurrency)
	var wg sync.WaitGroup

	start := time.Now()

	for psv := range entries {
		wg.Add(1)
		sem <- struct{}{} // acquire
		go func(psv *PSV) {
			var value []byte
			err := db.session.Query(`SELECT v FROM kv WHERE p = ? AND s = ?`, psv.p, psv.s).Consistency(gocql.One).Scan(&value)

			if err != nil {
				fmt.Printf("select error for p=%s s=%s: %w", psv.p, psv.s, err)
				os.Exit(1) // FIXME: too lazy to do this better at the moment
			}

			<-sem // release
			wg.Done()
		}(psv)
	}

	elapsed := time.Since(start).Seconds()
	return int(float64(db.opsPerIter) / elapsed), nil
}

func (db *DB) SelectMany() (int, error) {
	totalScanned := atomic.Int64{}
	var wg sync.WaitGroup
	start := time.Now()

	for range db.concurrency {
		wg.Add(1)
		go func() {
			numScanned := int64(0)
			partition := fmt.Sprintf("part-%d", randv2.IntN(db.partitions))
			iter := db.session.Query(`SELECT s, v FROM kv WHERE p = ? LIMIT ?`, partition, db.opsPerIter/db.concurrency).Consistency(gocql.One).Iter()
			var s string
			var v []byte
			for iter.Scan(&s, &v) {
				numScanned++
			}

			wg.Done()
			totalScanned.Add(numScanned)
		}()
	}

	wg.Wait()
	elapsed := time.Since(start).Seconds()

	// not returning this because the numbers are high enough that it throws the graphs off
	// return int(float64(totalScanned.Load()) / elapsed), nil

	// instead return the rate of total scans / elapsed time
	return int(float64(db.concurrency) / elapsed), nil
}

func (db *DB) InterTestDelay() {
	if db.delay > 0 {
		time.Sleep(db.delay)
	}
}

func (db *DB) DumpPartitionSizes() error {
	total := 0

	for i := 0; i < db.partitions; i++ {
		partition := fmt.Sprintf("part-%d", i)
		var count int
		err := db.session.Query(`SELECT COUNT(*) FROM kv WHERE p = ?`, partition).Scan(&count)
		if err != nil {
			return err
		}

		fmt.Printf("Partition %s size: %d\n", partition, count)
		total += count
	}

	fmt.Printf("Total kvs: %d\n", total)

	return nil
}

type PSV struct {
	p string
	s string
	v []byte
}

func (db *DB) PsvGenerator(startingRows int64, includeValue bool) chan *PSV {
	// NOTE generator needs to be deterministic so that select queries will work

	// Create a Zipf distribution to introduce hotspots
	rng := rand.New(rand.NewSource(int64(startingRows)))
	zipf := rand.NewZipf(rng, 2.0, 1.0, uint64(db.partitions-1))
	// Use Go 2 RNG for deterministic hotspot selection
	rngv2 := randv2.New(randv2.NewPCG(uint64(startingRows), uint64(startingRows)*2+1))
	hotspot := rngv2.IntN(db.partitions)

	// Sort keys will be pseudo-random letters
	sortGenerator := NewLetterSequence(0)
	sortGenerator.Seed(uint64(startingRows))

	// Values will be pseudo-random bytes
	valueGenerator := NewByteSequence(0)
	valueGenerator.Seed(uint64(startingRows))

	ch := make(chan *PSV, db.opsPerIter)

	go func() {
		for i := 0; i < db.opsPerIter; i++ {
			psv := &PSV{}

			// Compute an offset from the hotspot using Zipf and wrap into valid range
			offset := int(zipf.Uint64()) % db.partitions
			psv.p = fmt.Sprintf("part-%d", (hotspot+offset)%db.partitions)
			psv.s = sortGenerator.Letters(64)

			if includeValue {
				psv.v = make([]byte, db.valueLen)
				valueGenerator.Fill(psv.v)
			}

			ch <- psv
		}
		close(ch)
	}()

	return ch
}

var schema = []string{
	`CREATE TABLE kv (
	   p text,
  	   s text,
  	   v blob,
       PRIMARY KEY (p, s)
	);`,
	`ALTER TABLE kv WITH compaction = {'class': 'LeveledCompactionStrategy'};`,
	`CREATE TABLE rows (
    	id int primary key, -- always 0
    	rows bigint
    );`,
	`INSERT INTO rows (id, rows) VALUES (0, 0);`,
}
