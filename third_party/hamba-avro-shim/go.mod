// Local replacement module for github.com/hamba/avro/v2.
//
// The upstream github.com/hamba/avro/v2 module is archived and carries three
// unfixed advisories (GO-2026-5046/5047/5048). Its maintained successor is
// github.com/iskorotkov/avro/v2, which fixes them in v2.33.0+. That successor
// renamed its module path, so a plain `replace hamba => iskorotkov` is rejected
// by the go tool ("used for two different module paths") and would fork the
// Avro type universe between iceberg-go (imports the hamba path) and our code.
//
// This module keeps the hamba import path but contains only type aliases and
// function forwarders to github.com/iskorotkov/avro/v2. Aliases preserve type
// identity, so every consumer (iceberg-go + wallaby) compiles against a single,
// patched Avro implementation. See README.md for full provenance.
module github.com/hamba/avro/v2

go 1.24.13

require github.com/iskorotkov/avro/v2 v2.33.1

require (
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
)
