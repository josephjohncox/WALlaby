package s3

import (
	"context"
	"errors"
	"io"
	"strconv"
	"testing"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

func BenchmarkDestinationWrite(b *testing.B) {
	for _, format := range []string{"json", "parquet"} {
		b.Run(format+"/new", func(b *testing.B) {
			client := &benchmarkObjectClient{}
			destination := testDestination(b, client, format, "")
			batch := s3TestBatch()

			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				batch.Checkpoint.LSN = strconv.Itoa(index + 1)
				if err := destination.Write(context.Background(), batch); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if got := client.puts; got != b.N {
				b.Fatalf("object writes = %d, want %d", got, b.N)
			}
			b.ReportMetric(float64(client.puts)/float64(b.N), "object-requests/op")
		})

		b.Run(format+"/replay", func(b *testing.B) {
			client := newFakeObjectClient()
			destination := testDestination(b, client, format, "")
			batch := s3TestBatch()
			if err := destination.Write(context.Background(), batch); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := destination.Write(context.Background(), batch); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if got := client.objectCount(); got != 1 {
				b.Fatalf("objects = %d, want 1", got)
			}
			requests := client.putCount() + client.headCount()
			b.ReportMetric(float64(requests-1)/float64(b.N), "object-requests/op")
		})
	}
}

type benchmarkObjectClient struct {
	puts int
}

func (c *benchmarkObjectClient) PutObject(_ context.Context, input *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	if _, err := io.Copy(io.Discard, input.Body); err != nil {
		return nil, err
	}
	c.puts++
	return &awss3.PutObjectOutput{}, nil
}

func (*benchmarkObjectClient) HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	return nil, errors.New("unexpected reconciliation in new-write benchmark")
}
