// etcd's Go client, sort and revision filters on Range, and its
// concurrency package's Mutex and Election on fastetcd (fastetcd#154).
//
//	clientv3_concurrency <endpoint>
//
// Prints "ok <what>" per check and exits 1 at the first failure.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func fail(f string, a ...any) {
	fmt.Printf("FAIL "+f+"\n", a...)
	os.Exit(1)
}

func keys(r *clientv3.GetResponse) string {
	var k []string
	for _, kv := range r.Kvs {
		k = append(k, string(kv.Key))
	}
	return strings.Join(k, ",")
}

func main() {
	ep := os.Args[1]
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{ep}, DialTimeout: 5 * time.Second})
	if err != nil {
		fail("connect: %v", err)
	}
	ctx := context.Background()

	// Created in the order c, a, b: key order is not creation order.
	for _, k := range []string{"s/c", "s/a", "s/b"} {
		if _, err := c.Put(ctx, k, k); err != nil {
			fail("put %s: %v", k, err)
		}
	}
	get := func(what, want string, opts ...clientv3.OpOption) *clientv3.GetResponse {
		r, err := c.Get(ctx, "s/", append([]clientv3.OpOption{clientv3.WithPrefix()}, opts...)...)
		if err != nil {
			fail("%s: %v", what, err)
		}
		if got := keys(r); got != want {
			fail("%s: got %s, want %s", what, got, want)
		}
		fmt.Println("ok", what, "=", want)
		return r
	}
	all := get("get prefix (key order)", "s/a,s/b,s/c")
	createdA := all.Kvs[0].CreateRevision
	get("sort by create descending", "s/b,s/a,s/c", clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortDescend))
	get("first create (WithFirstCreate)", "s/c", clientv3.WithFirstCreate()...)
	r := get("last create (WithLastCreate)", "s/b", clientv3.WithLastCreate()...)
	if !r.More || r.Count != 3 {
		fail("last create: more=%v count=%d, want true 3", r.More, r.Count)
	}
	get("max create revision", "s/a,s/c", clientv3.WithMaxCreateRev(createdA))
	get("min mod revision", "s/a,s/b", clientv3.WithMinModRev(createdA))
	get("sort by value descending", "s/c,s/b,s/a", clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend))

	// Two sessions; b's lease is granted second, so its key sorts after
	// a's: "pfx/<lease id hex>".
	sa, err := concurrency.NewSession(c, concurrency.WithTTL(30))
	if err != nil {
		fail("session a: %v", err)
	}
	sb, err := concurrency.NewSession(c, concurrency.WithTTL(30))
	if err != nil {
		fail("session b: %v", err)
	}
	ka, kb := fmt.Sprintf("lock/%x", sa.Lease()), fmt.Sprintf("lock/%x", sb.Lease())
	if !(ka < kb) {
		fail("expected a's key %s to sort before b's %s", ka, kb)
	}
	ma, mb := concurrency.NewMutex(sa, "lock"), concurrency.NewMutex(sb, "lock")
	if err := mb.Lock(ctx); err != nil {
		fail("b locks: %v", err)
	}
	fmt.Println("ok b holds the lock")
	if err := ma.TryLock(ctx); !errors.Is(err, concurrency.ErrLocked) {
		fail("a's TryLock while b holds the lock: got %v, want %v (mutual exclusion)", err, concurrency.ErrLocked)
	}
	fmt.Println("ok a's TryLock says locked")
	short, cancel := context.WithTimeout(ctx, time.Second)
	err = ma.Lock(short)
	cancel()
	if err == nil {
		fail("a's Lock returned while b holds the lock")
	}
	fmt.Println("ok a's Lock waits while b holds it")
	got := make(chan error, 1)
	go func() { got <- ma.Lock(ctx) }()
	time.Sleep(300 * time.Millisecond)
	if err := mb.Unlock(ctx); err != nil {
		fail("b unlocks: %v", err)
	}
	select {
	case err := <-got:
		if err != nil {
			fail("a locks after b unlocked: %v", err)
		}
	case <-time.After(10 * time.Second):
		fail("a did not get the lock after b unlocked")
	}
	fmt.Println("ok a gets the lock once b unlocks")
	_ = ma.Unlock(ctx)

	// Election: b campaigns first and leads; a waits behind it.
	ea, eb := concurrency.NewElection(sa, "elect"), concurrency.NewElection(sb, "elect")
	if err := eb.Campaign(ctx, "b"); err != nil {
		fail("b campaigns: %v", err)
	}
	won := make(chan error, 1)
	go func() { won <- ea.Campaign(ctx, "a") }()
	time.Sleep(500 * time.Millisecond)
	select {
	case <-won:
		fail("a was elected while b leads")
	default:
	}
	l, err := ea.Leader(ctx)
	if err != nil || string(l.Kvs[0].Value) != "b" {
		fail("leader while b leads: %v %v", l, err)
	}
	fmt.Println("ok b leads, a waits")
	if err := eb.Resign(ctx); err != nil {
		fail("b resigns: %v", err)
	}
	select {
	case err := <-won:
		if err != nil {
			fail("a's campaign: %v", err)
		}
	case <-time.After(10 * time.Second):
		fail("a was not elected after b resigned")
	}
	l, _ = eb.Leader(ctx)
	if string(l.Kvs[0].Value) != "a" {
		fail("leader after b resigned: %s", l.Kvs[0].Value)
	}
	fmt.Println("ok a leads once b resigns")
}
