// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	Servers            = "servers"
	Topic              = "topic"
	Acks               = "acks"
	DeliveryTimeout    = "deliveryTimeout"
	ReadFromBeginning  = "readFromBeginning"
	ClientCert         = "clientCert"
	ClientKey          = "clientKey"
	CACert             = "caCert"
	InsecureSkipVerify = "insecureSkipVerify"
	SASLMechanism      = "saslMechanism"
	SASLUsername       = "saslUsername"
	SASLPassword       = "saslPassword"
)

var (
	SASLMechanismValues = []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}
	Required            = []string{Servers, Topic}
)

// Config contains all the possible configuration parameters for Kafka sources and destinations.
// When changing this struct, please also change the plugin specification (in main.go) as well as the ReadMe.
type Config struct {
	// A list of bootstrap servers, which will be used to discover all the servers in a cluster.
	Servers []string
	Topic   string
	// Required acknowledgments when writing messages to a topic:
	// Can be: none, one, all
	Acks            kafka.RequiredAcks
	DeliveryTimeout time.Duration
	// Read all messages present in a source topic.
	// Default value: false (only new messages are read)
	ReadFromBeginning bool
	// TLS section
	// The Kafka client's certificate
	ClientCert string
	// The Kafka client's private key
	ClientKey string
	// The Kafka broker's certificate
	CACert string
	// Whether or not to validate the broker's certificate chain and host name.
	// If `true`, accepts any certificate presented by the server and any host name in that certificate.
	InsecureSkipVerify bool
	// SASL section
	// possible values: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string
}

func (c *Config) useTLS() bool {
	return c.ClientCert != "" || c.CACert != "" || c.brokerHasCACert()
}

// brokerHasCACert determines if the broker's certificate has been signed by a CA.
// Returns `true` if we are confident that the broker uses a CA-signed cert.
// Returns `false` otherwise, which includes cases where we are not sure (e.g. server offline).
func (c *Config) brokerHasCACert() bool {
	conn, err := net.Dial("tcp", c.Servers[0])
	if err != nil {
		return false
	}
	cfg, err := newTLSConfig("", "", "", true)
	if err != nil {
		return false
	}
	client := tls.Client(conn, cfg)
	return client.Handshake() == nil
}

func (c *Config) saslEnabled() bool {
	return c.SASLUsername != "" && c.SASLPassword != ""
}

func Parse(cfg map[string]string) (Config, error) {
	err := checkRequired(cfg)
	// todo check if values are valid, e.g. hosts are valid etc.
	if err != nil {
		return Config{}, err
	}
	// parse servers
	servers, err := split(cfg[Servers])
	if err != nil {
		return Config{}, fmt.Errorf("invalid servers: %w", err)
	}
	var parsed = Config{
		Servers: servers,
		Topic:   cfg[Topic],
	}
	// parse acknowledgment setting
	ack, err := parseAcks(cfg[Acks])
	if err != nil {
		return Config{}, fmt.Errorf("couldn't parse ack: %w", err)
	}
	parsed.Acks = ack

	// parse and validate ReadFromBeginning
	readFromBeginning, err := parseBool(cfg, ReadFromBeginning, false)
	if err != nil {
		return Config{}, fmt.Errorf("invalid value for ReadFromBeginning: %w", err)
	}
	parsed.ReadFromBeginning = readFromBeginning

	// parse and validate delivery DeliveryTimeout
	timeout, err := parseDuration(cfg, DeliveryTimeout, 10*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("invalid delivery timeout: %w", err)
	}
	// it makes no sense to expect a message to be delivered immediately
	if timeout == 0 {
		return Config{}, errors.New("invalid delivery timeout: has to be > 0ms")
	}
	parsed.DeliveryTimeout = timeout

	// Security related settings
	err = setTLSConfigs(&parsed, cfg)
	if err != nil {
		return Config{}, fmt.Errorf("invalid TLS config: %w", err)
	}
	err = setSASLConfigs(&parsed, cfg)
	if err != nil {
		return Config{}, fmt.Errorf("invalid SASL config: %w", err)
	}
	return parsed, nil
}

func setSASLConfigs(parsed *Config, cfg map[string]string) error {
	var missingCreds []string
	if cfg[SASLUsername] == "" {
		missingCreds = append(missingCreds, SASLUsername)
	}
	if cfg[SASLPassword] == "" {
		missingCreds = append(missingCreds, SASLPassword)
	}
	if len(missingCreds) == 1 {
		return fmt.Errorf("SASL configuration incomplete, %v is missing", missingCreds[0])
	}
	mechanism, mechanismPresent := cfg[SASLMechanism]
	// Mechanism specified, but credentials haven't been provided.
	// Handles specifically the case where neither a username nor a password
	// have been provided.
	if mechanismPresent && len(missingCreds) != 0 {
		return errors.New("SASL mechanism provided, but username and password are missing")
	}

	if mechanism == "" {
		mechanism = "PLAIN"
	}
	if !validSASLMechanism(mechanism) {
		return fmt.Errorf("invalid SASL mechanism %q, expected one of: %v", mechanism, SASLMechanismValues)
	}
	parsed.SASLMechanism = mechanism
	parsed.SASLUsername = cfg[SASLUsername]
	parsed.SASLPassword = cfg[SASLPassword]
	return nil
}

func validSASLMechanism(mechanism string) bool {
	for _, v := range SASLMechanismValues {
		if v == mechanism {
			return true
		}
	}
	return false
}

func setTLSConfigs(parsed *Config, cfg map[string]string) error {
	// Get client TLS settings
	var missingClient []string
	if cfg[ClientCert] == "" {
		missingClient = append(missingClient, ClientCert)
	}
	if cfg[ClientKey] == "" {
		missingClient = append(missingClient, ClientKey)
	}
	if len(missingClient) == 1 {
		return fmt.Errorf("client TLS configuration incomplete, %v is missing", missingClient[0])
	}
	parsed.ClientCert = cfg[ClientCert]
	parsed.ClientKey = cfg[ClientKey]

	// Get server CA
	if caCert, ok := cfg[CACert]; ok {
		parsed.CACert = caCert
	}

	// Parse InsecureSkipVerify, default is 'false'
	insecureString, ok := cfg[InsecureSkipVerify]
	if ok {
		insecure, err := strconv.ParseBool(insecureString)
		if err != nil {
			return fmt.Errorf("value %q for InsecureSkipVerify is not valid", insecureString)
		}
		parsed.InsecureSkipVerify = insecure
	}
	return nil
}

func parseAcks(ack string) (kafka.RequiredAcks, error) {
	// when ack is empty, return default (which is 'all')
	if ack == "" {
		return kafka.RequireAll, nil
	}
	acks := kafka.RequiredAcks(0)
	err := acks.UnmarshalText([]byte(ack))
	if err != nil {
		return 0, fmt.Errorf("unknown ack mode: %w", err)
	}
	return acks, nil
}

func parseBool(cfg map[string]string, key string, defaultVal bool) (bool, error) {
	boolString, exists := cfg[key]
	if !exists {
		return defaultVal, nil
	}
	parsed, err := strconv.ParseBool(boolString)
	if err != nil {
		return false, fmt.Errorf("value for key %s cannot be parsed: %w", key, err)
	}
	return parsed, nil
}

func parseDuration(cfg map[string]string, key string, defaultVal time.Duration) (time.Duration, error) {
	timeoutStr, exists := cfg[key]
	if !exists {
		return defaultVal, nil
	}
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return 0, fmt.Errorf("duration cannot be parsed: %w", err)
	}
	return timeout, nil
}

func checkRequired(cfg map[string]string) error {
	for _, reqKey := range Required {
		_, exists := cfg[reqKey]
		if !exists {
			return requiredConfigErr(reqKey)
		}
	}
	return nil
}

func requiredConfigErr(name string) error {
	return fmt.Errorf("%q config value must be set", name)
}

func split(serversString string) ([]string, error) {
	split := strings.Split(serversString, ",")
	servers := make([]string, 0)
	for i, s := range split {
		if strings.Trim(s, " ") == "" {
			return nil, fmt.Errorf("empty %d. server", i)
		}
		servers = append(servers, s)
	}
	return servers, nil
}
