package main

import (
	"fmt"
	"io"
	"net/http"
	"sync"
)

// Structure to store results
type FetchResult struct {
	URL        string
	StatusCode int
	Size       int
	Error      error
}

// Worker function
func worker(id int, jobs <-chan string, results chan<- FetchResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for url := range jobs {
		resp, err := http.Get(url)
		result := FetchResult{URL: url}

		if err != nil {
			result.Error = err
			results <- result
			continue
		}

		result.StatusCode = resp.StatusCode

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			result.Error = err
		} else {
			result.Size = len(body)
		}

		results <- result
	}
}

func main() {
	fmt.Println("Fetching URLs concurrently using worker pool...\n")

	urls := []string{
		"https://example.com",
		"https://uottawa.ca",
		"https://golang.org",
		"https://httpbin.org/get",
		"https://github.com",
	}

	numWorkers := 5

	jobs := make(chan string, len(urls))
	results := make(chan FetchResult, len(urls))

	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i, jobs, results, &wg)
	}

	// Send jobs
	for _, url := range urls {
		jobs <- url
	}
	close(jobs)

	// Wait for workers to finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Print results
	for r := range results {
		if r.Error != nil {
			fmt.Printf("%s | ERROR: %v\n", r.URL, r.Error)
		} else {
			fmt.Printf("%s | Status: %d | Size: %d bytes\n",
				r.URL, r.StatusCode, r.Size)
		}
	}

	fmt.Println("\nScraping complete!")
}
