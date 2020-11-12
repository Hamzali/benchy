package main

import (
	"os"
	"testing"
)

func TestBenchy(t *testing.T) {
	os.Args = []string{
		"benchy",
		"-config",
		"../config.json",
		"-file",
		"../test_data/query_params.csv",
	}

	main()
}
