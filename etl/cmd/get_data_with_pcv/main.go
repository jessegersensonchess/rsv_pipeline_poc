// Package get_data_with_pcv processes a data file and matches integers in each
// line with a list of integers stored in a bitmap. It uses concurrency
// (goroutines) to speed up matching. It outputs matched lines if the verbose
// flag is set.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"etl/internal/bitmap"
	intscan "etl/internal/parser/ints"
)

// LoadIntsFromFile reads integers from a file, one per line, and returns them
// in a bitmap. Lines that cannot be parsed as integers are skipped.
func LoadIntsFromFile(filename string, maxID int) (*bitmap.Bitmap, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bm := bitmap.New(maxID)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		num, err := strconv.Atoi(line)
		if err != nil {
			// Skip non-integer lines
			continue
		}
		bm.Add(num)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return bm, nil
}

// processLine processes a single line of the data file and checks if any
// integer in the line is present in the bitmap. It safely increments the
// matchedRows counter if a match is found and optionally prints the line if
// verbose mode is enabled.
func processLine(
	line string,
	bm *bitmap.Bitmap,
	matchedRows *int,
	mu *sync.Mutex,
	verbose bool,
) {
	ints, err := intscan.ExtractInts(line)
	if err != nil {
		// For this implementation, err is always nil; if it ever isn't, we
		// treat it as "drop this line".
		return
	}
	if len(ints) == 0 {
		return
	}

	for _, num := range ints {
		if bm.Has(num) {
			mu.Lock()
			*matchedRows++
			mu.Unlock()

			if verbose {
				fmt.Println(line)
			}
			return
		}
	}
}

// CheckDataFileAgainstBitmap processes the data file in parallel using a
// fixed-size worker pool and checks for matches. It returns the number of
// matched rows.
func CheckDataFileAgainstBitmap(
	filename string,
	bm *bitmap.Bitmap,
	numWorkers int,
	verbose bool,
) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	lines := make(chan string, 100)
	var wg sync.WaitGroup

	matchedRows := 0
	var mu sync.Mutex

	// Start worker goroutines.
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range lines {
				processLine(line, bm, &matchedRows, &mu, verbose)
			}
		}()
	}

	// Feed lines to workers.
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines <- scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		close(lines)
		wg.Wait()
		return 0, err
	}

	// No more lines.
	close(lines)
	wg.Wait()

	return matchedRows, nil
}

func main() {
	start := time.Now()

	pcvFile := flag.String("pcvfile", "", "File containing a list of integers (one per line)")
	dataFile := flag.String("data", "", "File containing lines of text to check")
	numWorkers := flag.Int("workers", runtime.NumCPU()/2, "Number of workers to process data file concurrently")
	maxID := flag.Int("maxid", 150000000, "Maximum ID value for the bitmap")
	verbose := flag.Bool("verbose", false, "Enable verbose output to print matching lines")
	flag.Parse()

	if *pcvFile == "" || *dataFile == "" {
		fmt.Println("Both -pcvfile and -data flags are required.")
		return
	}

	// Step 1: Load integers from the PCV file and create the bitmap.
	bm, err := LoadIntsFromFile(*pcvFile, *maxID)
	if err != nil {
		fmt.Println("Error loading integers from file:", err)
		return
	}
	fmt.Println("DONE: LoadIntsFromFile", time.Since(start))

	// Step 2: Check the data file against the bitmap using concurrency.
	matchedRows, err := CheckDataFileAgainstBitmap(*dataFile, bm, *numWorkers, *verbose)
	if err != nil {
		fmt.Println("Error processing data file:", err)
		return
	}

	fmt.Printf("DONE: %d matched rows\n", matchedRows)
	fmt.Println("Total time:", time.Since(start))
}
