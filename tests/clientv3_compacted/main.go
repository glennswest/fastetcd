// clientv3 gets ErrCompacted, once, for a watch at a compacted revision
// (fastetcd#222).
//
//	clientv3_compacted <endpoint>
//
// Puts k three times, compacts at the second put, then watches k from
// the first put's revision. etcd's client must deliver one response whose
// Err() is ErrCompacted (CompactRevision set) and then close the channel.
// Before #222 fastetcd answered with one created-and-canceled response
// carrying a reason: clientv3 took it for a failed create and re-created
// the watch in a loop, so the channel stayed silent. A watch from the
// compact revision itself is the control: it is not compacted and gets
// the third put.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	ep := os.Args[1]
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{ep}, DialTimeout: 5 * time.Second})
	if err != nil {
		fail("connect: %v", err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var revs []int64
	for _, v := range []string{"1", "2", "3"} {
		r, err := cli.Put(ctx, "k", v)
		if err != nil {
			fail("put: %v", err)
		}
		revs = append(revs, r.Header.Revision)
	}
	if _, err := cli.Compact(ctx, revs[1], clientv3.WithCompactPhysical()); err != nil {
		fail("compact: %v", err)
	}

	// Below the compact revision: ErrCompacted, then the channel closes.
	wctx, wcancel := context.WithTimeout(ctx, 10*time.Second)
	defer wcancel()
	wch := cli.Watch(wctx, "k", clientv3.WithRev(revs[0]))
	select {
	case r, ok := <-wch:
		if !ok {
			fail("compacted watch: channel closed with no response")
		}
		if !errors.Is(r.Err(), rpctypes.ErrCompacted) {
			fail("compacted watch: Err() = %v, want ErrCompacted (response %+v)", r.Err(), r)
		}
		if r.CompactRevision != revs[1] {
			fail("compacted watch: CompactRevision %d, want %d", r.CompactRevision, revs[1])
		}
		fmt.Printf("ok compacted watch: %v (compact revision %d)\n", r.Err(), r.CompactRevision)
	case <-wctx.Done():
		fail("compacted watch: no response in 10 s (the client is re-creating the watch, #222)")
	}
	select {
	case r, ok := <-wch:
		if ok {
			fail("compacted watch: another response after ErrCompacted: %+v (err %v)", r, r.Err())
		}
		fmt.Println("ok compacted watch: channel closed after ErrCompacted")
	case <-wctx.Done():
		fail("compacted watch: channel still open 10 s after ErrCompacted")
	}

	// Control: from the compact revision, history is still there.
	cctx, ccancel := context.WithTimeout(ctx, 10*time.Second)
	defer ccancel()
	select {
	case r := <-cli.Watch(cctx, "k", clientv3.WithRev(revs[1])):
		if r.Err() != nil || len(r.Events) == 0 {
			fail("control watch: err %v, %d events", r.Err(), len(r.Events))
		}
		fmt.Printf("ok control watch from the compact revision: %d events, first at %d\n",
			len(r.Events), r.Events[0].Kv.ModRevision)
	case <-cctx.Done():
		fail("control watch: nothing in 10 s")
	}
}

func fail(f string, a ...any) {
	fmt.Printf("FAIL "+f+"\n", a...)
	os.Exit(1)
}
