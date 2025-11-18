// Package get_data_with_pcv processes a data file and matches integers in each line with a list of integers stored in a bitmap.
// It uses concurrency (goroutines) to speed up matching. It outputs matched lines if the verbose flag is set.
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
	"unicode"
)

// Bitmap struct represents a bitmap that holds bit values in a slice of uint64 integers.
// The bitmap is used to store which IDs are present (set bit = present).
type Bitmap struct {
	data []uint64 // Slice holding the bitmap data
}

// NewBitmap allocates a bitmap that can store bits for IDs [0, maxID].
func NewBitmap(maxID int) *Bitmap {
	if maxID <= 0 {
		return &Bitmap{data: nil} // No memory allocation needed for maxID <= 0
	}

	// Calculate the number of uint64 'egg cartons' needed
	eggCartons := (maxID + 63) / 64 // Correct the calculation to avoid an extra egg carton when maxID is a multiple of 64

	return &Bitmap{
		data: make([]uint64, eggCartons),
	}
}

// Add sets the bit corresponding to an ID in the bitmap.
func (bm *Bitmap) Add(id int) {
	if id < 0 {
		return // Ignore negative IDs
	}
	eggCarton := id / 64           // Determine which egg carton (uint64) holds the bit for this ID
	bit := uint(id % 64)           // Determine the bit position within the egg carton
	bm.data[eggCarton] |= 1 << bit // Set the corresponding bit to 1
}

// Has checks if the bitmap contains a given ID by checking if the corresponding bit is set.
func (bm *Bitmap) Has(id int) bool {
	if id < 0 {
		return false // Negative IDs are not considered
	}
	eggCarton := id / 64 // Determine which egg carton holds the 'egg' (bit) for this ID
	if eggCarton < 0 || eggCarton >= len(bm.data) {
		return false // If the egg carton is out of bounds, the ID is not in the bitmap
	}
	bit := uint(id % 64)                          // Determine the bit position within the egg carton
	return (bm.data[eggCarton] & (1 << bit)) != 0 // Return true if there is an egg in that position (i.e. if bit is set), otherwise false
}

// ExtractInts extracts integers from a string by parsing all sequences of digits.
// It returns a slice of integers extracted from the input string.
func ExtractInts(s string) ([]int, error) {
	var ints []int        // Slice to hold the parsed integers
	var currentNum []rune // Temporarily holds the current number as a sequence of characters

	// Iterate through each character in the string
	for _, char := range s {
		if unicode.IsDigit(char) {
			// If the character is a digit, add it to the current number
			currentNum = append(currentNum, char)
		} else {
			// If we encounter a non-digit character and there's a current number
			if len(currentNum) > 0 {
				// Convert the current number (rune slice) to a string, then to an integer
				numStr := string(currentNum)
				num, err := strconv.Atoi(numStr)
				if err != nil {
					// If there's an error converting to an integer, return nil, nil
					return nil, nil
				}
				// Append the valid integer to the result slice
				ints = append(ints, num)
				// Reset currentNum to start capturing the next number
				currentNum = nil
			}
		}
	}

	// If there's a number left at the end of the string, add it to the result
	if len(currentNum) > 0 {
		numStr := string(currentNum)
		num, err := strconv.Atoi(numStr)
		if err != nil {
			// Return nil, nil for out-of-range errors or non-integer data
			return nil, nil
		}
		ints = append(ints, num)
	}

	return ints, nil
}

// LoadIntsFromFile reads integers from a file, one per line, and returns them as a Bitmap.
// It processes the file line-by-line, adds the integers to the bitmap, and returns the bitmap containing the set bits.
func LoadIntsFromFile(filename string, maxID int) (*Bitmap, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err // Return error if file cannot be opened
	}
	defer file.Close()

	// Create a new bitmap to store integers
	bitmap := NewBitmap(maxID)
	scanner := bufio.NewScanner(file)

	// Read the file line by line
	for scanner.Scan() {
		line := scanner.Text()
		num, err := strconv.Atoi(line) // Convert the line to an integer
		if err != nil {
			// If conversion fails, skip this line (non-integer line)
			continue
		}
		// Add the integer to the bitmap
		bitmap.Add(num)
	}

	if err := scanner.Err(); err != nil {
		return nil, err // Return error if scanning the file fails
	}

	return bitmap, nil // Return the populated bitmap
}

// processLine processes a single line of the data file and checks if any integer in the line is present in the bitmap.
// It safely increments the matchedRows counter if a match is found and optionally prints the line if verbose mode is enabled.
func processLine(line string, bitmap *Bitmap, matchedRows *int, mu *sync.Mutex, verbose bool) {
	// Extract integers from the line
	ints, err := ExtractInts(line)
	if err != nil {
		// If error occurs, skip the line (non-integer data)
		return
	}

	// Check if any integer in the line exists in the bitmap
	for _, num := range ints {
		if bitmap.Has(num) {
			// If a match is found, safely increment the matched rows counter
			mu.Lock() // Lock to ensure thread safety when updating matchedRows
			*matchedRows++
			mu.Unlock()

			// If verbose flag is set, print the matching line
			if verbose {
				fmt.Println(line)
			}
			return
		}
	}
}

// CheckDataFileAgainstBitmap processes the data file in parallel using a fixed-size worker pool and checks for matches.
func CheckDataFileAgainstBitmap(filename string, bitmap *Bitmap, numWorkers int, verbose bool) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err // Return error if file cannot be opened
	}
	defer file.Close()

	// Channel to send lines to workers
	resultChan := make(chan string, 100)

	// WaitGroup to wait for all workers to complete their tasks
	var wg sync.WaitGroup
	matchedRows := 0
	var mu sync.Mutex // Mutex for safely updating matchedRows counter

	// Create a worker pool: spawn numWorkers goroutines to process lines
	for i := 0; i < numWorkers; i++ {
		go func() {
			// Each worker processes lines from the resultChan
			for line := range resultChan {
				processLine(line, bitmap, &matchedRows, &mu, verbose)
				wg.Done() // Signal that the task is done
			}
		}()
	}

	// Scanner to read the file line-by-line
	scanner := bufio.NewScanner(file)

	// Read the file line-by-line and send each line to the workers
	for scanner.Scan() {
		line := scanner.Text()
		wg.Add(1) // Add a task to the WaitGroup for each line
		resultChan <- line
	}

	// Check for any errors during scanning
	if err := scanner.Err(); err != nil {
		return 0, err
	}

	// Close the result channel after all lines are sent to workers
	close(resultChan)

	// Wait for all workers to finish processing
	wg.Wait()

	return matchedRows, nil // Return the total matched rows
}

// main is the entry point for the program. It parses command-line flags, loads integers from the PCV file,
// and processes the data file to find matching rows using a bitmap.
func main() {
	start := time.Now()

	// Define flags for input files and settings
	pcvFile := flag.String("pcvfile", "", "File containing a list of integers (one per line)")
	dataFile := flag.String("data", "", "File containing lines of text to check")
	numWorkers := flag.Int("workers", runtime.NumCPU()/2, "Number of workers to process data file concurrently")
	maxID := flag.Int("maxid", 150000000, "Maximum ID value for the bitmap")
	verbose := flag.Bool("verbose", false, "Enable verbose output to print matching lines")
	flag.Parse()

	// Ensure both required flags are provided
	if *pcvFile == "" || *dataFile == "" {
		fmt.Println("Both -pcvfile and -data flags are required.")
		return
	}

	// Step 1: Load integers from the PCV file and create the bitmap
	bitmap, err := LoadIntsFromFile(*pcvFile, *maxID)
	if err != nil {
		fmt.Println("Error loading integers from file:", err)
		return
	}
	fmt.Println("DONE: LoadIntsFromFile", time.Since(start))

	// Step 2: Check the data file against the bitmap using concurrency
	matchedRows, err := CheckDataFileAgainstBitmap(*dataFile, bitmap, *numWorkers, *verbose)
	if err != nil {
		fmt.Println("Error processing data file:", err)
		return
	}

	// Output the summary of matched rows
	fmt.Printf("DONE: %d matched rows\n", matchedRows)
	fmt.Println("Total time:", time.Since(start))
}
