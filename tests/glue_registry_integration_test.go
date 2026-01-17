package tests

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/smithy-go"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
)

func TestGlueRegistryIntegration(t *testing.T) {
	endpoint := os.Getenv("WALLABY_TEST_GLUE_ENDPOINT")
	region := os.Getenv("WALLABY_TEST_GLUE_REGION")
	if endpoint == "" {
		t.Skip("WALLABY_TEST_GLUE_ENDPOINT not set")
	}
	if region == "" {
		region = "us-east-1"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	glueClient := glue.NewFromConfig(awsCfg, func(o *glue.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	registryName := "wallaby"
	_, err = glueClient.CreateRegistry(ctx, &glue.CreateRegistryInput{
		RegistryName: aws.String(registryName),
	})
	if err != nil {
		if isAlreadyExistsRegistry(err) {
			// ok
		} else if isGlueNotImplemented(err) {
			t.Skipf("glue registry not supported by localstack: %v", err)
		} else {
			t.Fatalf("create registry: %v", err)
		}
	}

	reg, err := schemaregistry.NewRegistry(ctx, schemaregistry.Config{
		Type:         "glue",
		Region:       region,
		Endpoint:     endpoint,
		GlueRegistry: registryName,
	})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	defer reg.Close()

	schema := `{"type":"record","name":"Test","fields":[{"name":"id","type":"string"}]}`
	res, err := reg.Register(ctx, schemaregistry.RegisterRequest{
		Subject:    "wallaby.test",
		Schema:     schema,
		SchemaType: schemaregistry.SchemaTypeAvro,
	})
	if err != nil {
		t.Fatalf("register schema: %v", err)
	}
	if res.Version == 0 {
		t.Fatalf("expected version > 0, got %d", res.Version)
	}
}

func isAlreadyExistsRegistry(err error) bool {
	var apiErr smithy.APIError
	if err == nil || !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode() == "AlreadyExistsException"
}

func isGlueNotImplemented(err error) bool {
	var apiErr smithy.APIError
	if err == nil || !errors.As(err, &apiErr) {
		return false
	}
	if apiErr.ErrorCode() != "InternalFailure" {
		return false
	}
	return strings.Contains(strings.ToLower(apiErr.ErrorMessage()), "not yet implemented")
}
