package klog

import (
	"github.com/humboldt-xie/klog/data"
	"testing"
)

func TestKlog(t *testing.T) {
	log := KLog{}
	log.Open("testA")
	address := []byte("hello")
	defer log.Remove("testA")
	//log.Put(address, 0, &data.Data{})
	for i := 0; i <= 1000; i++ {
		log.Sequence(address).Put(address, &data.Data{Key: []byte("hello")})
	}
	seq, _ := log.LastSequence(address)
	if seq != 1000 {
		t.Fatalf("sequence not match %d", seq)
	}
	ran, err := log.GetRange(address, 900, 1000, &data.Data{})
	if err != nil || len(ran) != 100 {
		t.Fatalf("err:%s len %d", err, len(ran))
	}
	d := ran[0].(*data.Data)
	if d == nil {
		t.Fatalf("data not impl %#v", d)
	}
}
