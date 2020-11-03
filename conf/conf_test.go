package conf_test

import (
	"reflect"
	"testing"

	"github.com/hamzali/benchy/conf"
)

func TestInitConfig(t *testing.T) {
	confPath := "../config.json"
	fileConfig := conf.Config{}

	err := conf.ReadConfig(confPath, &fileConfig)
	if err != nil {
		t.Fatalf("could not read config file: %v", err)
	}

	changedFileConfig := fileConfig
	changedFileConfig.Postgres.Port = 5555

	changedDefaultConfig := conf.DefaultConfig
	changedDefaultConfig.WorkerCount = 10
	changedDefaultConfig.Postgres.Host = "some.host"

	tt := []struct {
		name           string
		args           []string
		expectedConfig conf.Config
	}{
		{
			"should return default config without flags",
			[]string{},
			conf.DefaultConfig,
		},
		{
			"should read given flag",
			[]string{"-worker", "10", "-host", "some.host"},
			changedDefaultConfig,
		},
		{
			"should read config file",
			[]string{"-config", confPath},
			fileConfig,
		},
		{
			"should override config file if flag provided",
			[]string{"-config", confPath, "-port", "5555"},
			changedFileConfig,
		},
	}

	for _, tc := range tt {
		args := tc.args
		expected := tc.expectedConfig
		t.Run(tc.name, func(st *testing.T) {
			config, err := conf.InitConfig("benchy", args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)

				return
			}

			if !reflect.DeepEqual(config, &expected) {
				t.Fatalf("expected %v but got %v", config, expected)
			}
		})
	}
}
