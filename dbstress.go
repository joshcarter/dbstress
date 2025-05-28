package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

const (
	Insert      = 0
	Select      = 1
	SelectMulti = 2
)

type DB struct {
	*log.Logger
	session     *gocql.Session
	concurrency int
	partitions  int
	valueLen    int
	opsPerIter  int
	rows        int64
	notify      chan os.Signal
	bwLog       io.WriteCloser
}

func main() {
	// Connect to Cassandra
	cluster := gocql.NewCluster("127.0.0.1") // Seed node
	cluster.Port = 9042
	cluster.Keyspace = "dbstress"
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = 5 * time.Second
	cluster.DisableInitialHostLookup = true
	cluster.IgnorePeerAddr = true
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
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
		concurrency: 100,
		partitions:  100,
		valueLen:    1024,
		opsPerIter:  1000,
		rows:        0,
		notify:      make(chan os.Signal, 1),
		bwLog:       bwLog,
	}

	err = db.PreStart()

	if err != nil {
		log.Fatalf("Unable to init rows: %s\n", err)
	}

	fmt.Printf("==== dbstress starting ====\n")
	fmt.Printf(" starting rows: %d\n", db.rows)
	fmt.Printf(" value len:     %d\n", db.valueLen)

	// db.DumpPartitionSizes()
	// return

	// Register for control-C
	signal.Notify(db.notify, os.Interrupt)

	for {
		select {
		case <-db.notify:
			fmt.Printf("Control-C; exiting with %d rows.\n", db.rows)
			return
		default:
			irate, err := db.Insert()

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			srate := 0
			srate2 := 0

			if db.rows%10000 == 0 {
				srate, err = db.SelectOne()

				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}

				srate2, err = db.SelectMany()

				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			}

			if srate > 0 {
				_, _ = fmt.Fprintf(db.bwLog, "%d, %d, %d, %d\n", db.rows, irate, srate, srate2)
				fmt.Printf(" - %d rows, insert %d rows/sec, select %d rows/sec, many %d rows/sec\n", db.rows, irate, srate, srate2)
			} else {
				_, _ = fmt.Fprintf(db.bwLog, "%d, %d, NA, NA\n", db.rows, irate)
				fmt.Printf(" - %d rows, insert %d rows/sec\n", db.rows, irate)
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

func (db *DB) Insert() (int, error) {
	entries := db.PsvGenerator(db.rows, true)
	sem := make(chan struct{}, db.concurrency)
	var wg sync.WaitGroup

	start := time.Now()

	for psv := range entries {
		wg.Add(1)
		sem <- struct{}{} // acquire
		go func(psv *PSV) {
			err := db.session.Query(`INSERT INTO kv (p, s, v) VALUES (?, ?, ?)`,
				psv.p, psv.s, psv.v).Exec()

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
	err := db.session.Query(`UPDATE rows SET rows = ? WHERE id = ?`, db.rows, 0).Exec()

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
			err := db.session.Query(`SELECT v FROM kv WHERE p = ? AND s = ?`, psv.p, psv.s).Scan(&value)

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
	partition := fmt.Sprintf("part-%d", rand.IntN(db.partitions))
	numScanned := 0
	start := time.Now()

	iter := db.session.Query(`SELECT s, v FROM kv WHERE p = ? LIMIT ?`, partition, db.opsPerIter).Iter()
	var s string
	var v []byte
	for iter.Scan(&s, &v) {
		numScanned++
	}

	elapsed := time.Since(start).Seconds()
	return int(float64(numScanned) / elapsed), nil
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
	// These need to be deterministic so that select queries will work
	partitionGenerator := NewNumberSequence()
	partitionGenerator.Seed(int64(startingRows))
	sortGenerator := NewLetterSequence(0)
	sortGenerator.Seed(uint64(startingRows))
	valueGenerator := NewByteSequence(0)
	valueGenerator.Seed(uint64(startingRows))

	ch := make(chan *PSV, db.opsPerIter)

	go func() {
		for i := 0; i < db.opsPerIter; i++ {
			psv := &PSV{}

			psv.p = fmt.Sprintf("part-%d", int(partitionGenerator.Next())%db.partitions)
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

// `CREATE KEYSPACE IF NOT EXISTS dbstress WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '2' };`,
// `USE dbstress;`,

var schema = []string{
	`CREATE TABLE kv (
	   p text,
  	   s text,
  	   v blob,
       PRIMARY KEY (p, s)
	);`,
	`CREATE TABLE rows (
    	id int primary key, -- always 0
    	rows bigint
    );`,
	`INSERT INTO rows (id, rows) VALUES (0, 0);`,
}
