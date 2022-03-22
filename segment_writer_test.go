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
	"testing"

	"github.com/matryer/is"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

func TestSegmentWriter_TLSOnly(t *testing.T) {
	is := is.New(t)

	caCert := "Bag Attributes\n    friendlyName: caroot\n    2.16.840.1.113894.746875.1.1: <Unsupported tag 6>\nsubject=/CN=localhost/OU=CIA/O=REA/L=Melbourne/C=AU\nissuer=/CN=localhost/OU=CIA/O=REA/L=Melbourne/C=AU\n-----BEGIN CERTIFICATE-----\nMIIDHjCCAgYCCQC9iilqJUAoxzANBgkqhkiG9w0BAQsFADBRMRIwEAYDVQQDDAls\nb2NhbGhvc3QxDDAKBgNVBAsMA0NJQTEMMAoGA1UECgwDUkVBMRIwEAYDVQQHDAlN\nZWxib3VybmUxCzAJBgNVBAYTAkFVMB4XDTIyMDMyMTEyNDcyNloXDTQ5MDgwNTEy\nNDcyNlowUTESMBAGA1UEAwwJbG9jYWxob3N0MQwwCgYDVQQLDANDSUExDDAKBgNV\nBAoMA1JFQTESMBAGA1UEBwwJTWVsYm91cm5lMQswCQYDVQQGEwJBVTCCASIwDQYJ\nKoZIhvcNAQEBBQADggEPADCCAQoCggEBAK8eUXQWIEvqH6gWg9CyU1FZ9I5ZfeSz\n5BgEwQelG3YRiBmN4MXQmVErvy8JEdC9AbDdNvwsWlBD1xoWC0S2Q2qMhF6M03ny\nrrx0OwKNxdNwZvrMCin6adVS66x4R83X/YprZiS0fMtZHnrPsEVZxw7QSObGPnUV\nqinVqZh4Mo2N7tbxYa6ZALXgDf0yXbzGOGENuEaw9+5H01+6wDAwoxmm3pgQ0bF1\nyrqGh6P5ePtbaI6C+WBW/u0HgXUgyJaQA3vZIS6cOnwf76osPkFiCp5LtjTWblBd\nBwEmjtMu4n6/QEsUNM93lP/iJ7y8LWGrMFxL1700KFWlkJ8CIbpdjH0CAwEAATAN\nBgkqhkiG9w0BAQsFAAOCAQEAAJXIy4onOdKSceG3gjJHjCIZf74/Ka7rIhCnVyn1\n/EiTub70Df1b2vsPl25axv3ujMETubUzSzyyRSQ/5o61T5nZXPmmbTtU/7f1lLWk\n6tiHyKRCPd6InLnGFqhjmKp052LCwX0ZqUy2x+/uXxrYI7+wcB9QpDQsw4nnfDhb\nkym8hzUIu6IST1TbHFD7NoTA1L/qVlVQ8Sj6XVGQKmwBPxX/i1wHFTDrnS4uskvK\nFkPUsd6OmYbguwS3Ktj40C/pV3Z5OS/kR4+pO349I+b42ImzWxpgMRSVuI4y0Lvk\nm6GdbnJPvzqZT5yZmv05j6LQXkm5ugqPmOARMKrSrWACUA==\n-----END CERTIFICATE-----\n"
	clientKeyPem := "Bag Attributes\n    friendlyName: broker\n    localKeyID: 54 69 6D 65 20 31 36 34 37 38 36 37 32 30 38 36 35 35 \nKey Attributes: <No Attributes>\n-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCQTuVvpsn8Eu3b\nerPVw576+bifl9QJTtDdaYHeywDRmrp3XO7VUOsRf6Wacm+4+2Uvbi0NVrhWbLwe\n7/jNoOhgDXCS85A6FnZU+LBfwJBLBJjPFZ654rFMjz+kmuHf2b5J7LkqXAuSTAFe\nW11EiMchiREg9inhoxwGt2qviEUYabZLJR31pr2qd7jGiLK9EPY3y8UaW/15HUXq\nQhWVntAQPFTtitnue7fet4OwcY8nCkJ5yOnxRznX78k877ycfgKLJwHkETwHWp2x\nXwJyTz0mJ0nhbYadacGHieKgqu1zUC9f7Tz4AUOB+coEALqKnRPz9nKmT4lUQwD+\nzOr7tmMxAgMBAAECggEAabZyGuV6577SIcr0PG8OYlpXJgoqGRt0pA3rRlM96U5I\ntLIOf5PEb9Ard0XHlCINUL6MIE5bwWvsL1mp0LDEKcEOq4fjKrpTuxFm2u4Mhff7\nHRCAczmemjAB9kpDlyFCZZMVXfOJwoUNJ5sUauUrwuRO+O97ZMCBAmaQr7/KpgOI\n6OxPYv0WE6uqaijFWp7HnXc5Xcw0zo5riEh+5ZxjaehJFWoFpOKpOgL+pkMqfAZb\nVGTY8BxRJA11nR/Do34kz8WahX6Us3DCMpMMWJcg8ss80JRFyj30o683tOfBeTdP\nHVJrRz1GdW7wHnGR94D/a3TuUISJ1U++GUMdXmQNFQKBgQDHrgG2wwzGQ8Uv+cWD\nLj02flAA6Xrln4JQpelrQZrrYcYbPevTordeeXnY+COtGF04ix5dQWsWAgYlRSnr\n1s56en+RsnhvNggoHz48nPXNwbp/XZx1or8fJUMwLicwosFtLF7DxLMF4tq1pdkX\nZrr9UJO7LtirgAOqiZzNsd9xOwKBgQC5AsFh7CHSwNo/x6G6XJ9c4hL0dYeuxbbo\n4a08NageTRWXxBdpkYn4YBrLk49jA3JnDrfJuJfJ7jQdGBLwNdTFWZp+wB/QT2su\nkE2Ur+ysid95xLM1aCw17RfzpSXhkhMUJTRSYgIyqkkClRnRqhUKDowyt0AfdzCW\nGAPTdac2gwKBgCquDr+5wSk/ow42HPmFEKBtLzyCqzoZdgk27UV3qF1XcLix6444\n4WjYHis6HqYI5yQG2F6mdPUnSZj9x5AZQdj8BfhmZUegDO5Gf08FXaS1G9/Nanva\nZW+Kz2mk88t5fk6PhVHi4UEI1CavZE+ULbOnXWxM/xLpMd9pupJcyp2xAoGALs+Z\nqnMao76T+itCqmqhD9lLvnq2V+xCuW3QbTmOTgxm+D1vRxDB/gwi+3tcfkry+Uxq\nCCoijb8thGcA87JLIZvoUUW/Ru+xSNjOKF7S3V0NJDw2s76l4QcaVlVk3kwdc61u\nLaIKuFMJohOjsr78D81af8KKAOwhaPiujyRnqI0CgYAmyjg1o85Od/Q0MwM2zQ9w\ng14mYRjaLCvvYdCe62tw9B8JTfO5n6kIFokCUJY6lsZbjE4su2sES5VftoizUk2p\njGoznm1taoeEby59583cf79ijBWj+gt8KZl3KvM1vjN55lZ4mUS8yIDzzlShoXOf\nexA8AP+RDi6/PwW4SYQpmg==\n-----END PRIVATE KEY-----\n"
	clientCerPem := "Bag Attributes\n    friendlyName: broker\n    localKeyID: 54 69 6D 65 20 31 36 34 37 38 36 37 32 30 38 36 35 35 \nsubject=/C=AU/ST=VIC/L=Melbourne/O=REA/OU=CIA/CN=localhost\nissuer=/CN=localhost/OU=CIA/O=REA/L=Melbourne/C=AU\n-----BEGIN CERTIFICATE-----\nMIIDLDCCAhQCCQCGHfkQtzwLDTANBgkqhkiG9w0BAQUFADBRMRIwEAYDVQQDDAls\nb2NhbGhvc3QxDDAKBgNVBAsMA0NJQTEMMAoGA1UECgwDUkVBMRIwEAYDVQQHDAlN\nZWxib3VybmUxCzAJBgNVBAYTAkFVMB4XDTIyMDMyMTEyNDcyN1oXDTQ5MDgwNTEy\nNDcyN1owXzELMAkGA1UEBhMCQVUxDDAKBgNVBAgTA1ZJQzESMBAGA1UEBxMJTWVs\nYm91cm5lMQwwCgYDVQQKEwNSRUExDDAKBgNVBAsTA0NJQTESMBAGA1UEAxMJbG9j\nYWxob3N0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAkE7lb6bJ/BLt\n23qz1cOe+vm4n5fUCU7Q3WmB3ssA0Zq6d1zu1VDrEX+lmnJvuPtlL24tDVa4Vmy8\nHu/4zaDoYA1wkvOQOhZ2VPiwX8CQSwSYzxWeueKxTI8/pJrh39m+Sey5KlwLkkwB\nXltdRIjHIYkRIPYp4aMcBrdqr4hFGGm2SyUd9aa9qne4xoiyvRD2N8vFGlv9eR1F\n6kIVlZ7QEDxU7YrZ7nu33reDsHGPJwpCecjp8Uc51+/JPO+8nH4CiycB5BE8B1qd\nsV8Cck89JidJ4W2GnWnBh4nioKrtc1AvX+08+AFDgfnKBAC6ip0T8/Zypk+JVEMA\n/szq+7ZjMQIDAQABMA0GCSqGSIb3DQEBBQUAA4IBAQAh1sobuLh1uN6qZJOvV6vS\nG2a182VhX0ktBxXTZyXshSKa0lT93vtPgMEz/xRQ3H6ZVdEh6+GbY7jLIYjiqFhm\neuDnb6A+SP1VdosSPY6pg9tNWVIcrVTeUltrbJGYp7HTyaAvgqc5fzinhEmvbwxr\n/A3LBUjr/WrwzTCq/lwhQwjE61EET9SV/fzcF+I8I8SF5uVwHEnlyV9FaFYg36Ba\nZBt3Mfvf1Ai477N3npv7j9OoenCTxuIr0jrQJ7QT1pq1wEjckSAbuzpqqg0TXy1l\nXBkCk5xXHRriCnPBtL6VMlnLwpxap1nOTHWkp70z8HerW44+GfnoMI67G5Q16GBX\n-----END CERTIFICATE-----\nBag Attributes\n    friendlyName: C=AU,L=Melbourne,O=REA,OU=CIA,CN=localhost\nsubject=/CN=localhost/OU=CIA/O=REA/L=Melbourne/C=AU\nissuer=/CN=localhost/OU=CIA/O=REA/L=Melbourne/C=AU\n-----BEGIN CERTIFICATE-----\nMIIDHjCCAgYCCQC9iilqJUAoxzANBgkqhkiG9w0BAQsFADBRMRIwEAYDVQQDDAls\nb2NhbGhvc3QxDDAKBgNVBAsMA0NJQTEMMAoGA1UECgwDUkVBMRIwEAYDVQQHDAlN\nZWxib3VybmUxCzAJBgNVBAYTAkFVMB4XDTIyMDMyMTEyNDcyNloXDTQ5MDgwNTEy\nNDcyNlowUTESMBAGA1UEAwwJbG9jYWxob3N0MQwwCgYDVQQLDANDSUExDDAKBgNV\nBAoMA1JFQTESMBAGA1UEBwwJTWVsYm91cm5lMQswCQYDVQQGEwJBVTCCASIwDQYJ\nKoZIhvcNAQEBBQADggEPADCCAQoCggEBAK8eUXQWIEvqH6gWg9CyU1FZ9I5ZfeSz\n5BgEwQelG3YRiBmN4MXQmVErvy8JEdC9AbDdNvwsWlBD1xoWC0S2Q2qMhF6M03ny\nrrx0OwKNxdNwZvrMCin6adVS66x4R83X/YprZiS0fMtZHnrPsEVZxw7QSObGPnUV\nqinVqZh4Mo2N7tbxYa6ZALXgDf0yXbzGOGENuEaw9+5H01+6wDAwoxmm3pgQ0bF1\nyrqGh6P5ePtbaI6C+WBW/u0HgXUgyJaQA3vZIS6cOnwf76osPkFiCp5LtjTWblBd\nBwEmjtMu4n6/QEsUNM93lP/iJ7y8LWGrMFxL1700KFWlkJ8CIbpdjH0CAwEAATAN\nBgkqhkiG9w0BAQsFAAOCAQEAAJXIy4onOdKSceG3gjJHjCIZf74/Ka7rIhCnVyn1\n/EiTub70Df1b2vsPl25axv3ujMETubUzSzyyRSQ/5o61T5nZXPmmbTtU/7f1lLWk\n6tiHyKRCPd6InLnGFqhjmKp052LCwX0ZqUy2x+/uXxrYI7+wcB9QpDQsw4nnfDhb\nkym8hzUIu6IST1TbHFD7NoTA1L/qVlVQ8Sj6XVGQKmwBPxX/i1wHFTDrnS4uskvK\nFkPUsd6OmYbguwS3Ktj40C/pV3Z5OS/kR4+pO349I+b42ImzWxpgMRSVuI4y0Lvk\nm6GdbnJPvzqZT5yZmv05j6LQXkm5ugqPmOARMKrSrWACUA==\n-----END CERTIFICATE-----\nBag Attributes\n    friendlyName: caroot\n    2.16.840.1.113894.746875.1.1: <Unsupported tag 6>\nsubject=/CN=localhost/OU=CIA/O=REA/L=Melbourne/C=AU\nissuer=/CN=localhost/OU=CIA/O=REA/L=Melbourne/C=AU\n-----BEGIN CERTIFICATE-----\nMIIDHjCCAgYCCQC9iilqJUAoxzANBgkqhkiG9w0BAQsFADBRMRIwEAYDVQQDDAls\nb2NhbGhvc3QxDDAKBgNVBAsMA0NJQTEMMAoGA1UECgwDUkVBMRIwEAYDVQQHDAlN\nZWxib3VybmUxCzAJBgNVBAYTAkFVMB4XDTIyMDMyMTEyNDcyNloXDTQ5MDgwNTEy\nNDcyNlowUTESMBAGA1UEAwwJbG9jYWxob3N0MQwwCgYDVQQLDANDSUExDDAKBgNV\nBAoMA1JFQTESMBAGA1UEBwwJTWVsYm91cm5lMQswCQYDVQQGEwJBVTCCASIwDQYJ\nKoZIhvcNAQEBBQADggEPADCCAQoCggEBAK8eUXQWIEvqH6gWg9CyU1FZ9I5ZfeSz\n5BgEwQelG3YRiBmN4MXQmVErvy8JEdC9AbDdNvwsWlBD1xoWC0S2Q2qMhF6M03ny\nrrx0OwKNxdNwZvrMCin6adVS66x4R83X/YprZiS0fMtZHnrPsEVZxw7QSObGPnUV\nqinVqZh4Mo2N7tbxYa6ZALXgDf0yXbzGOGENuEaw9+5H01+6wDAwoxmm3pgQ0bF1\nyrqGh6P5ePtbaI6C+WBW/u0HgXUgyJaQA3vZIS6cOnwf76osPkFiCp5LtjTWblBd\nBwEmjtMu4n6/QEsUNM93lP/iJ7y8LWGrMFxL1700KFWlkJ8CIbpdjH0CAwEAATAN\nBgkqhkiG9w0BAQsFAAOCAQEAAJXIy4onOdKSceG3gjJHjCIZf74/Ka7rIhCnVyn1\n/EiTub70Df1b2vsPl25axv3ujMETubUzSzyyRSQ/5o61T5nZXPmmbTtU/7f1lLWk\n6tiHyKRCPd6InLnGFqhjmKp052LCwX0ZqUy2x+/uXxrYI7+wcB9QpDQsw4nnfDhb\nkym8hzUIu6IST1TbHFD7NoTA1L/qVlVQ8Sj6XVGQKmwBPxX/i1wHFTDrnS4uskvK\nFkPUsd6OmYbguwS3Ktj40C/pV3Z5OS/kR4+pO349I+b42ImzWxpgMRSVuI4y0Lvk\nm6GdbnJPvzqZT5yZmv05j6LQXkm5ugqPmOARMKrSrWACUA==\n-----END CERTIFICATE-----\n"

	config := Config{
		Servers:            []string{"test-host:9092"},
		Topic:              "test-topic",
		CACert:             caCert,
		ClientKey:          clientKeyPem,
		ClientCert:         clientCerPem,
		InsecureSkipVerify: true,
	}
	underTest, err := newWriter(config)
	is.NoErr(err)

	transport, ok := underTest.Transport.(*kafka.Transport)
	is.True(ok)
	is.True(transport.TLS != nil)
	is.True(transport.TLS.InsecureSkipVerify == config.InsecureSkipVerify)
}

func TestSegmentWriter_SASLOnly(t *testing.T) {
	is := is.New(t)
	config := Config{
		Servers:      []string{"test-host:9092"},
		Topic:        "test-topic",
		SASLUsername: "sasl-username",
		SASLPassword: "sasl-password",
	}
	underTest, err := newWriter(config)
	is.NoErr(err)

	transport, ok := underTest.Transport.(*kafka.Transport)
	is.True(ok)

	mechanism := transport.SASL
	is.True(mechanism != nil)

	plainMechanism, ok := mechanism.(plain.Mechanism)
	is.True(ok)
	is.Equal(config.SASLUsername, plainMechanism.Username)
	is.Equal(config.SASLPassword, plainMechanism.Password)
}
