package bus

import "testing"

func TestValidateBusName(t *testing.T) {
	if err := validateBusName("sys.order.paid"); err != nil {
		t.Fatalf("expected valid bus name, got %v", err)
	}
	if err := validateBusName("_data.cache.invalidate"); err != nil {
		t.Fatalf("expected internal bus name to be allowed, got %v", err)
	}
	if err := validateBusName("_sys.announce"); err == nil {
		t.Fatal("expected reserved prefix error")
	}
}

func TestDecodeRequestRejectsReservedName(t *testing.T) {
	data, err := encodeRequest(nil, "_sys.announce", nil)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	if _, _, _, err = decodeRequest(data); err == nil {
		t.Fatal("expected decode to reject reserved bus name")
	}
}

func TestDecodeRequestAllowsInternalName(t *testing.T) {
	data, err := encodeRequest(nil, "_data.cache.invalidate", nil)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	if _, _, _, err = decodeRequest(data); err != nil {
		t.Fatalf("expected decode to allow internal bus name, got %v", err)
	}
}
