package connector

import "testing"

func TestDeliveryConfigFingerprintIsStableAndSensitive(t *testing.T) {
	first, err := DeliveryConfigFingerprint(Spec{
		Name: "target",
		Type: EndpointPostgres,
		Options: map[string]string{
			"dsn":                     "postgres://one",
			"schema":                  "public",
			"destination_revision_id": "revision-a",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	reordered, err := DeliveryConfigFingerprint(Spec{
		Name: "target",
		Type: EndpointPostgres,
		Options: map[string]string{
			"destination_revision_id": "revision-b",
			"schema":                  "public",
			"dsn":                     "postgres://one",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first != reordered {
		t.Fatalf("revision identity must not change the config fingerprint: %q != %q", first, reordered)
	}
	changed, err := DeliveryConfigFingerprint(Spec{
		Name: "target",
		Type: EndpointPostgres,
		Options: map[string]string{
			"dsn":                     "postgres://two",
			"schema":                  "public",
			"destination_revision_id": "revision-a",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if changed == first {
		t.Fatal("configuration change must change the destination fingerprint")
	}
}
