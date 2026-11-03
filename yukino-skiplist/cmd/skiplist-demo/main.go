// Command skiplist-demo exercises the yukino-skiplist API end to end. It is
// the Go counterpart of the main.cpp driver shipped with the original
// Skiplist-CPP project, extended to cover persistence, ordered iteration and
// concurrent access.
//
// Diagnostic messages are written to standard error through log/slog, while
// the rendered data structure is written to standard output, so the two
// streams can be redirected independently.
package main

import (
	"cmp"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	skiplist "github.com/yukino-js/shita.go/yukino-skiplist"
)

// demoMaxLevel is a small lane cap and uses the same numeric argument as the
// original C++ driver. That driver treated the argument as the highest level
// index, so it allocated one extra lane; this program counts lanes.
const demoMaxLevel = 6

func main() {
	storePath := flag.String("store", filepath.Join("store", "dump.txt"),
		"path of the dump file used by the persistence step")
	workers := flag.Int("workers", 8,
		"number of goroutines used by the concurrency step")
	opsPerWorker := flag.Int("ops", 20_000,
		"number of operations performed by each goroutine in the concurrency step")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	if err := run(*storePath, *workers, *opsPerWorker); err != nil {
		slog.Error("demo failed", "error", err)
		os.Exit(1)
	}
}

func run(storePath string, workers, opsPerWorker int) error {
	if workers < 1 || opsPerWorker < 1 {
		return fmt.Errorf("workers and ops must be positive, got workers=%d ops=%d", workers, opsPerWorker)
	}

	list := skiplist.New[int, string](skiplist.WithMaxLevel(demoMaxLevel))

	insertion(list)
	lookup(list)
	if err := persistence(list, storePath); err != nil {
		return err
	}
	deletion(list)
	orderedViews(list)
	return concurrency(workers, opsPerWorker)
}

// section prints a heading that separates the demo steps.
func section(title string) {
	fmt.Printf("\n%s\n%s\n", title, strings.Repeat("-", len(title)))
}

func insertion(list *skiplist.SkipList[int, string]) {
	section("Insertion")

	seed := []struct {
		key   int
		value string
	}{
		{1, "learn"},
		{3, "algorithms"},
		{7, "skip"},
		{8, "list"},
		{9, "concurrency"},
		{19, "persistence"},
	}
	for _, element := range seed {
		fmt.Printf("Insert(%d, %q) added=%t\n", element.key, element.value,
			list.Insert(element.key, element.value))
	}

	// Insert never overwrites, which is what the C++ original did as well.
	fmt.Printf("Insert(19, %q) added=%t\n", "duplicate", list.Insert(19, "duplicate"))

	// Set does overwrite, and reports what it replaced. The C++ original had no
	// equivalent, so a value could never be corrected in place.
	previous, existed := list.Set(19, "durability")
	fmt.Printf("Set(19, %q) previous=%q existed=%t\n", "durability", previous, existed)

	fmt.Printf("\nsize=%d\n%s", list.Len(), list)
}

func lookup(list *skiplist.SkipList[int, string]) {
	section("Lookup")

	for _, key := range []int{9, 18} {
		value, ok := list.Get(key)
		fmt.Printf("Get(%d) value=%q found=%t\n", key, value, ok)
		fmt.Printf("Has(%d) present=%t\n", key, list.Has(key))
	}
}

func persistence(list *skiplist.SkipList[int, string], storePath string) error {
	section("Persistence")

	format := skiplist.NewFormat(skiplist.IntCodec(), skiplist.StringCodec())
	if err := list.DumpFile(storePath, format); err != nil {
		return fmt.Errorf("dumping to %s: %w", storePath, err)
	}
	slog.Info("dump written", "path", storePath, "records", list.Len())

	contents, err := os.ReadFile(storePath)
	if err != nil {
		return fmt.Errorf("reading %s: %w", storePath, err)
	}
	fmt.Printf("%s", contents)

	restored, err := skiplist.LoadFile[int, string](storePath, format,
		skiplist.WithMaxLevel(demoMaxLevel))
	if err != nil {
		return fmt.Errorf("loading %s: %w", storePath, err)
	}
	slog.Info("dump restored", "path", storePath, "records", restored.Len())

	if !slices.Equal(list.Snapshot(), restored.Snapshot()) {
		return fmt.Errorf("round trip mismatch: %v != %v", list.Snapshot(), restored.Snapshot())
	}
	fmt.Println("round trip verified: the restored list matches the original")

	// Values that contain the separator, a backslash or a line break survive
	// because both fields are escaped. The C++ original corrupted such records.
	tricky := skiplist.New[int, string](skiplist.WithMaxLevel(demoMaxLevel))
	tricky.Set(1, "colon:inside")
	tricky.Set(2, "back\\slash")
	tricky.Set(3, "line\nbreak")
	if err := tricky.DumpFile(storePath, format); err != nil {
		return fmt.Errorf("dumping escaped records: %w", err)
	}
	contents, err = os.ReadFile(storePath)
	if err != nil {
		return fmt.Errorf("reading %s: %w", storePath, err)
	}
	fmt.Printf("\nescaped records on disk:\n%s", contents)

	restoredTricky, err := skiplist.LoadFile[int, string](storePath, format)
	if err != nil {
		return fmt.Errorf("loading escaped records: %w", err)
	}
	if !slices.Equal(tricky.Snapshot(), restoredTricky.Snapshot()) {
		return fmt.Errorf("escaped round trip mismatch: %v != %v",
			tricky.Snapshot(), restoredTricky.Snapshot())
	}
	fmt.Println("escaped round trip verified")

	// Restore the primary dump so the remaining steps operate on real data.
	return list.DumpFile(storePath, format)
}

func deletion(list *skiplist.SkipList[int, string]) {
	section("Deletion")

	for _, key := range []int{3, 7, 404} {
		value, deleted := list.Delete(key)
		fmt.Printf("Delete(%d) value=%q deleted=%t\n", key, value, deleted)
	}

	fmt.Printf("\nsize=%d\n%s", list.Len(), list)
}

func orderedViews(list *skiplist.SkipList[int, string]) {
	section("Ordered views")

	var keys []int
	for key := range list.Keys() {
		keys = append(keys, key)
	}
	fmt.Printf("Keys()      %v\n", keys)

	fmt.Print("Range(5, 20) ")
	for key, value := range list.Range(5, 20) {
		fmt.Printf("%d=%s ", key, value)
	}
	fmt.Println()

	if key, value, ok := list.Min(); ok {
		fmt.Printf("Min()       %d=%s\n", key, value)
	}
	if key, value, ok := list.Max(); ok {
		fmt.Printf("Max()       %d=%s\n", key, value)
	}
	// 8 and 9 are present, so Floor returns those keys. 10 is absent, so Floor
	// returns 9, the greatest stored key that sorts at or before the probe.
	for _, probe := range []int{8, 9, 10} {
		if key, value, ok := list.Floor(probe); ok {
			fmt.Printf("Floor(%d) = %d=%s\n", probe, key, value)
		}
		if key, value, ok := list.Ceiling(probe); ok {
			fmt.Printf("Ceiling(%d) = %d=%s\n", probe, key, value)
		}
	}

	// Snapshot detaches the elements, so the list can be mutated while the
	// copy is being walked. Iterating All() and deleting from the body would
	// deadlock, because the iterator holds the shared lock.
	fmt.Print("Snapshot()  ")
	for _, entry := range list.Snapshot() {
		list.Delete(entry.Key)
		fmt.Printf("%d ", entry.Key)
	}
	fmt.Printf("\nsize after draining through a snapshot: %d\n", list.Len())
}

func concurrency(workers, opsPerWorker int) error {
	section("Concurrency")

	const maxLevel = 12
	list := skiplist.New[int, string](skiplist.WithMaxLevel(maxLevel))

	started := time.Now()
	var group sync.WaitGroup
	group.Add(workers)
	for worker := range workers {
		go func(worker int) {
			defer group.Done()
			for operation := range opsPerWorker {
				key := worker*opsPerWorker + operation
				switch operation % 4 {
				case 0:
					list.Set(key, fmt.Sprintf("worker-%d", worker))
				case 1:
					list.Get(key - 1)
				case 2:
					list.Insert(key, fmt.Sprintf("worker-%d", worker))
				case 3:
					list.Delete(key - 2)
				}
			}
		}(worker)
	}
	group.Wait()
	elapsed := time.Since(started)

	total := workers * opsPerWorker
	slog.Info("concurrent workload finished",
		"workers", workers,
		"operations", total,
		"elapsed", elapsed.Round(time.Microsecond),
		"throughput_ops_per_second", int64(float64(total)/elapsed.Seconds()),
		"remaining_elements", list.Len(),
		"height", list.Level(),
	)

	if !slices.IsSortedFunc(list.Snapshot(), func(a, b skiplist.Entry[int, string]) int {
		return cmp.Compare(a.Key, b.Key)
	}) {
		return errors.New("concurrent workload left the list out of order")
	}
	fmt.Println("the list stayed ordered under concurrent writers and readers")
	return nil
}
