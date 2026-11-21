package endpointcodec

import (
	"errors"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
)

func encodeIggy(out map[string]string, cfg *wallabypb.IggyDestinationConfig) error {
	if cfg == nil {
		return errors.New("iggy config is required")
	}
	put(out, "address", cfg.GetAddress())
	put(out, "stream", cfg.GetStream())
	put(out, "topic", cfg.GetTopic())
	putU32(out, "partition_id", cfg.PartitionId)
	put(out, "username", cfg.GetUsername())
	put(out, "password", cfg.GetPassword())
	put(out, "token", cfg.GetToken())
	putBool(out, "tls", cfg.Tls)
	put(out, "tls_ca_file", cfg.GetTlsCaFile())
	put(out, "tls_server_name", cfg.GetTlsServerName())
	putEnum(out, "format", wireFormat(cfg.GetFormat()))
	putU32(out, "max_message_bytes", cfg.MaxMessageBytes)
	if err := putPositiveDuration(out, "timeout", cfg.GetTimeout()); err != nil {
		return err
	}
	return encodeTypeMappings(out, cfg.GetTypeMappings())
}

func decodeIggy(v map[string]string) (*wallabypb.IggyDestinationConfig, error) {
	cfg := &wallabypb.IggyDestinationConfig{
		Address: take(v, "address"), Stream: take(v, "stream"), Topic: take(v, "topic"),
		Username: take(v, "username"), Password: take(v, "password"), Token: take(v, "token"),
		TlsCaFile: take(v, "tls_ca_file"), TlsServerName: take(v, "tls_server_name"),
	}
	var err error
	if cfg.PartitionId, err = takeU32(v, "partition_id"); err != nil {
		return nil, err
	}
	if cfg.Tls, err = takeBool(v, "tls"); err != nil {
		return nil, err
	}
	if cfg.Format, err = parseWireFormat(take(v, "format")); err != nil {
		return nil, err
	}
	if cfg.MaxMessageBytes, err = takeU32(v, "max_message_bytes"); err != nil {
		return nil, err
	}
	if cfg.Timeout, err = takePositiveDuration(v, "timeout"); err != nil {
		return nil, err
	}
	if cfg.TypeMappings, err = decodeTypeMappings(v); err != nil {
		return nil, err
	}
	return cfg, nil
}
