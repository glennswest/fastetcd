// etcd's own Go client (clientv3) against fastetcd, through a member
// restart that loses every auth token (fastetcd#105).
//
//	reauth setup <endpoint>   add user root (role root), enable auth
//	reauth run <endpoint>     as root: put; wait for a line on stdin
//	                          (the member restarts meanwhile); get and
//	                          put again on the same client
//
// clientv3 re-authenticates and retries only when the server answers
// etcd's exact ErrInvalidAuthToken / ErrUserEmpty text.
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func fail(what string, err error) {
	fmt.Printf("FAIL %s: %v\n", what, err)
	os.Exit(1)
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("usage: reauth setup|run <endpoint>")
		os.Exit(2)
	}
	cfg := clientv3.Config{Endpoints: []string{os.Args[2]}, DialTimeout: 5 * time.Second}
	ctx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 15*time.Second)
	}
	switch os.Args[1] {
	case "setup":
		c, err := clientv3.New(cfg)
		if err != nil {
			fail("connect", err)
		}
		defer c.Close()
		x, cancel := ctx()
		defer cancel()
		if _, err := c.RoleAdd(x, "root"); err != nil {
			fail("role add", err)
		}
		if _, err := c.UserAdd(x, "root", "rootpw"); err != nil { // not a secret: test fixture
			fail("user add", err)
		}
		if _, err := c.UserGrantRole(x, "root", "root"); err != nil {
			fail("grant", err)
		}
		if _, err := c.AuthEnable(x); err != nil {
			fail("auth enable", err)
		}
		fmt.Println("ok setup")
	case "run":
		cfg.Username, cfg.Password = "root", "rootpw" // not a secret: test fixture
		c, err := clientv3.New(cfg)
		if err != nil {
			fail("connect", err)
		}
		defer c.Close()
		x, cancel := ctx()
		if _, err := c.Put(x, "reauth/before", "1"); err != nil {
			fail("put before the restart", err)
		}
		cancel()
		fmt.Println("ok put before the restart")
		bufio.NewReader(os.Stdin).ReadString('\n')
		x, cancel = ctx()
		r, err := c.Get(x, "reauth/before")
		cancel()
		if err != nil {
			fail("get after the restart", err)
		}
		if len(r.Kvs) != 1 || string(r.Kvs[0].Value) != "1" {
			fail("get after the restart", fmt.Errorf("got %v", r.Kvs))
		}
		fmt.Println("ok get after the restart (token refreshed)")
		x, cancel = ctx()
		_, err = c.Put(x, "reauth/after", "2")
		cancel()
		if err != nil {
			fail("put after the restart", err)
		}
		fmt.Println("ok put after the restart")
	}
}
