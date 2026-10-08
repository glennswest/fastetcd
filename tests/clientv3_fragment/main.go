// clientv3 joins fastetcd's watch fragments (fastetcd#58).
//
//	clientv3_fragment <endpoint>
//
// A client whose receive limit is 2 MiB watches big/ twice, with and
// without clientv3.WithFragment, while one txn writes 8 values of
// 400 KiB (one 3.2 MiB watch response). With fragments the watch must
// deliver all 8 events in one response; without, the response is over
// the client's limit (reported, the control).
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	ep := os.Args[1]
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:          []string{ep},
		DialTimeout:        5 * time.Second,
		MaxCallRecvMsgSize: 2 << 20,
	})
	if err != nil {
		fail("connect: %v", err)
	}
	defer cli.Close()
	// The writer has no receive limit to prove; clientv3's send limit
	// (2 MiB by default) must take the 3.2 MiB txn.
	writer, err := clientv3.New(clientv3.Config{
		Endpoints:          []string{ep},
		DialTimeout:        5 * time.Second,
		MaxCallSendMsgSize: 8 << 20,
	})
	if err != nil {
		fail("connect: %v", err)
	}
	defer writer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	frag := cli.Watch(clientv3.WithRequireLeader(ctx), "big/", clientv3.WithPrefix(), clientv3.WithFragment())
	whole := clientv3.NewWatcher(cli).Watch(clientv3.WithRequireLeader(ctx), "big/", clientv3.WithPrefix())
	time.Sleep(500 * time.Millisecond)

	value := strings.Repeat("x", 400*1024)
	ops := make([]clientv3.Op, 8)
	for i := range ops {
		ops[i] = clientv3.OpPut(fmt.Sprintf("big/%d", i), value)
	}
	tr, err := writer.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		fail("txn: %v", err)
	}
	rev := tr.Header.Revision
	fmt.Printf("ok txn at revision %d (8 x 400 KiB)\n", rev)

	select {
	case r, ok := <-frag:
		if !ok {
			fail("fragmenting watch closed")
		}
		if err := r.Err(); err != nil {
			fail("fragmenting watch: %v", err)
		}
		if len(r.Events) != 8 {
			fail("fragmenting watch: %d events in one response, want 8", len(r.Events))
		}
		for i, ev := range r.Events {
			if string(ev.Kv.Key) != fmt.Sprintf("big/%d", i) || len(ev.Kv.Value) != len(value) || ev.Kv.ModRevision != rev {
				fail("fragmenting watch: event %d is %s rev %d", i, ev.Kv.Key, ev.Kv.ModRevision)
			}
		}
		fmt.Println("ok fragmenting watch: 8 events joined into one response")
	case <-ctx.Done():
		fail("fragmenting watch: nothing in 30 s")
	}

	select {
	case r, ok := <-whole:
		switch {
		case !ok:
			fmt.Println("control: watch without fragment closed")
		case r.Err() != nil:
			fmt.Printf("control: watch without fragment: %v\n", r.Err())
		default:
			fmt.Printf("control: watch without fragment got %d events\n", len(r.Events))
		}
	case <-time.After(10 * time.Second):
		fmt.Println("control: watch without fragment: nothing in 10 s")
	}
}

func fail(f string, a ...any) {
	fmt.Printf("FAIL "+f+"\n", a...)
	os.Exit(1)
}
