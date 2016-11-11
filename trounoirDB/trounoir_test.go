package trounoirDB

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConfig(t *testing.T) {
	cg := new(Config)
	err := cg.Parse("../example_config.json")
	assert.Nil(t, err, "parsing err")
	assert.Equal(t, 3, cg.CopyRange)
	assert.Equal(t, 57439, cg.Port)
	assert.Equal(t, "KWJBdH1VHryHEsns3ZrhqA2jJnjVzr", cg.Salt)

	items := []ConfigItem{
		{"192.168.7.0", false},
		{"192.168.7.1", false},
		{"192.168.7.2", true},
		{"192.168.7.3", false},
		{"192.168.7.4", false},
	}
	assert.Equal(t, items, cg.Items)

	local, err := cg.GetLocalConfig()
	assert.Nil(t, err, "local config err")
	assert.Equal(t, ConfigItem{"192.168.7.2", true}, local.Self)
	assert.Equal(t, []ConfigItem{{"192.168.7.3", false}, {"192.168.7.4", false}, {"192.168.7.0", false}}, local.Dup)
}

func TestRequest(t *testing.T) {
	rq := new(Request)
	rq.GenSecure("KWJBdH1VHryHEsns3ZrhqA2jJnjVzr")
	yes := rq.Verify("KWJBdH1VHryHEsns3ZrhqA2jJnjVzr")
	assert.True(t, yes, "request verified")
}
