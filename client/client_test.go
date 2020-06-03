package client_test

import (
	"bytes"
	"context"
	"github.com/sachaos/simple-kv/client"
	"github.com/sirupsen/logrus"
	"testing"
)

func TestKVClient(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	client, err := client.NewClient("localhost:16379")
	if err != nil {
		t.Fatal(err)
	}

	err = client.Set(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Fatal(err)
	}

	val, err := client.Get(context.Background(), "foo")
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(val, []byte("bar")) {
		t.Error("not expected value")
	}
}
