package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

const retryAfter = 100 * time.Millisecond

func drive(ctx context.Context, url string, clients int) {
	tr := &http.Transport{MaxIdleConnsPerHost: clients}
	c := &http.Client{Transport: tr, Timeout: 2 * time.Second}

	var wg sync.WaitGroup

	for range clients {
		wg.Go(func() { loop(ctx, c, url+"/work") })
	}

	wg.Wait()
	tr.CloseIdleConnections()
}

func loop(ctx context.Context, c *http.Client, target string) {
	for ctx.Err() == nil {
		if err := fetch(ctx, c, target); err != nil {
			select {
			case <-ctx.Done():
			case <-time.After(retryAfter):
			}
		}
	}
}

func fetch(ctx context.Context, c *http.Client, target string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}

	resp, err := c.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}

	return nil
}
