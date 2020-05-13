package redis

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

type RdMgr struct {
	pool *redis.Pool
	addr string
	auth string
}

func (mgr *RdMgr) GetRedis() (conn redis.Conn, err error) {
	conn = mgr.pool.Get()
	//defer conn.Close()
	if _, err := conn.Do("AUTH", mgr.auth); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func NewRdManager(addr, auth string) *RdMgr {
	rdm := &RdMgr{
		addr: addr,
		auth: auth,
	}
	rdm.pool = &redis.Pool{
		MaxIdle:     8,
		MaxActive:   64,
		IdleTimeout: 300 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", addr)
		},
	}
	return rdm
}

