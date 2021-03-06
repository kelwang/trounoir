package trounoirDB

import (
	"encoding/json"
	"errors"
	"io/ioutil"
)

var (
	errNoItems            = errors.New("No Items in the config")
	errSaltTooWeak        = errors.New("The salt require at least 30 characters")
	errCopyRangeTooLarge  = errors.New("The copy range is larger than the length of the total cluster, please adjust your copyrange")
	errTooManyLocalconfig = errors.New("More than one local config is found, make sure there is only one is local equals true")
	errLocalConfigUnfound = errors.New("Local Config is not found, make sure to check your islocal true for you local config")
)

// spread     1 2 3 4
// 1 -> 2 3
// 2 -> 3 4
// 3 -> 4 1
// 4 -> 1 2
//
// to scale up,
// 1) add cluster 5
// 2) add 5 to config file of 4
// 3) restart 4, during restarting 1 2 3 5 will be keep active
// 4) change config 3, restart 3, during restart 1 2 4 5 will be active
// 5) change config 2, restart 2, during restart 1 3 4 5 will be active
// 6) do the rest, config and restart one by one
//
// to scale down
// change config 3 dup, from 4 1 to 4 2, restart
// change config 4 dup, from 1 2 to 2 3, restart
// stop 1
// do the rest, config and restart one by one
type Config struct {
	Items     []ConfigItem `json:"items"`
	Port      int          `json:"port"`
	CopyRange int          `json:"copy_range"`
	Salt      string       `json:"salt"`
	Folder    string       `json:"folder"`
}

// parse the json file, return errors if found any
func (config *Config) Parse(config_path string) error {
	buf, err := ioutil.ReadFile(config_path)
	if err != nil {
		return err
	}
	err = json.Unmarshal(buf, config)
	if err != nil {
		return err
	}

	if len(config.Salt) < 30 {
		return errSaltTooWeak
	}

	if len(config.Items) == 0 {
		return errNoItems
	}

	if config.CopyRange > len(config.Items) {
		return errCopyRangeTooLarge
	}

	countLocal := 0
	for k := range config.Items {
		if config.Items[k].IsLocal {
			countLocal++
		}
	}
	if countLocal == 0 {
		return errLocalConfigUnfound
	}
	if countLocal > 1 {
		return errTooManyLocalconfig
	}

	return nil
}

// run at app start time, loop through
func (config *Config) GetLocalConfig() (*LocalConfig, error) {
	local := new(LocalConfig)
	l := len(config.Items)

	for i := range config.Items {
		if config.Items[i].IsLocal {
			local.Self = config.Items[i]
			for j := 1; j <= config.CopyRange; j++ {
				var t int
				if i+j < l {
					t = i + j
				} else {
					t = i + j - l
				}
				local.Dup = append(local.Dup, config.Items[t])
			}
			return local, nil
		}
	}

	return nil, errLocalConfigUnfound
}

// local config
// dup must exclude self
// for a forward request, send forward request to dup
type LocalConfig struct {
	Self ConfigItem
	Dup  []ConfigItem
}

// for example c1
// host:       192.168.0.12
// islocal:    true
type ConfigItem struct {
	Host    string `json:"host"`
	IsLocal bool   `json:"islocal"`
}
