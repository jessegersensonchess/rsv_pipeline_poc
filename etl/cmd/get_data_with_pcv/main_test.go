package main

import (
	"etl/internal/bitmap"
	intscan "etl/internal/parser/ints"
	"os"
	"testing"
)

// TestAddAndHas tests the Add and Has methods of the Bitmap.
func TestAddAndHas(t *testing.T) {
	tests := []struct {
		id       int
		expected bool
	}{
		{10, true},  // Add a valid ID
		{-1, false}, // Add a negative ID, which should be ignored
		{63, true},  // Add an ID at the boundary of one word
		{64, false}, // Test that 64 is not included without explicit addition
	}

	bitmap := bitmap.New(64)
	for _, test := range tests {
		if test.expected {
			bitmap.Add(test.id)
		}
		if got := bitmap.Has(test.id); got != test.expected {
			t.Errorf("For ID %d, expected %v, got %v", test.id, test.expected, got)
		}
	}
}

// TestExtractInts tests the ExtractInts function for extracting integers from strings.
func TestExtractInts(t *testing.T) {
	tests := []struct {
		input    string
		expected []int
	}{
		{"abc123xyz456", []int{123, 456}}, // Simple case with numbers
		{"abc", nil},                      // No numbers
		{"12345", []int{12345}},           // Single number
		{"1a2b3c", []int{1, 2, 3}},        // Multiple numbers
	}

	for _, test := range tests {
		got, _ := intscan.ExtractInts(test.input)
		if !equal(got, test.expected) {
			t.Errorf("For input %q, expected %v, got %v", test.input, test.expected, got)
		}
	}
}

// equal is a helper function to compare two slices of integers.
func equal(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// TestLoadIntsFromFile tests the LoadIntsFromFile function.
func TestLoadIntsFromFile(t *testing.T) {
	// Create a temporary test file with sample integers
	tmpfile, err := os.CreateTemp("", "pcvfile_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name()) // Clean up after test

	// Write some sample data to the file
	data := []string{"1", "2", "3", "4"}
	for _, line := range data {
		tmpfile.WriteString(line + "\n")
	}

	// Close the file before reopening it in the function
	tmpfile.Close()

	bitmap, err := LoadIntsFromFile(tmpfile.Name(), 10)
	if err != nil {
		t.Fatalf("Failed to load integers from file: %v", err)
	}

	// Check if the bitmap has the correct values
	for _, id := range []int{1, 2, 3, 4} {
		if !bitmap.Has(id) {
			t.Errorf("Bitmap does not contain expected ID: %d", id)
		}
	}
}

func TestCheckDataFileAgainstBitmap(t *testing.T) {
	tests := []struct {
		name           string
		fileContent    string
		expectedResult int
	}{
		{
			name:           "No match",
			fileContent:    "5\n6\n7\n8\n",
			expectedResult: 0,
		},
	}

	// Prepare the bitmap to match against
	bitmap := bitmap.New(10)
	bitmap.Add(1)
	bitmap.Add(2)
	bitmap.Add(3)

	for _, test := range tests {
		// Create a temporary data file
		tmpfile, err := os.CreateTemp("", "datafile_test")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmpfile.Name())

		// Write test data to the file
		tmpfile.WriteString(test.fileContent)
		tmpfile.Close()

		// Run the function to check the file
		matchedRows, err := CheckDataFileAgainstBitmap(tmpfile.Name(), bitmap, 2, false)
		if err != nil {
			t.Fatalf("Failed to process data file: %v", err)
		}

		// Verify the result
		if matchedRows != test.expectedResult {
			t.Errorf("For test %q, expected %d matched rows, got %d", test.name, test.expectedResult, matchedRows)
		}
	}
}
