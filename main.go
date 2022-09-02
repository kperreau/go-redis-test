package main

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/labstack/echo/v4"
	"strconv"
	"sync"
)

var ctx = context.Background()

func getRedisSync(rdb *redis.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		execRedisCmd(rdb)
		return c.String(200, "OK")
	}
}

func getRedisAsync(rdb *redis.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		execRedisCmdAsync(rdb)
		return c.String(200, "OK")
	}
}

func getOk(c echo.Context) error {
	return c.String(200, "OK")
}

func redisConnection() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
		PoolSize: 1000,
	})

	return rdb
}

func execRedisCmdPipe(rdb *redis.Client) {
	pipe := rdb.Pipeline()
	for i := 0; i < 250; i++ {
		err := pipe.HMGet(ctx, "feeds-friends:"+strconv.Itoa(i), "post", "comments", "realmojis", "updatedAt").Err()
		if err != nil {
			panic(err)
		}
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		panic(err)
	}
}

type Content struct {
	Post      string `redis:"post"`
	Comments  string `redis:"comments"`
	Realmojis string `redis:"realmojis"`
	UpdatedAt string `redis:"updatedAt"`
}

func execRedisCmdAsync(rdb *redis.Client) {
	var wg sync.WaitGroup
	for i := 0; i < 250; i++ {
		wg.Add(1)
		go func(i int) {
			var content Content
			if err := rdb.HMGet(ctx, "feeds-friends:"+strconv.Itoa(i), "post", "comments", "realmojis", "updatedAt").Scan(&content); err != nil {
				panic(err)
			}
			defer wg.Done()
		}(i)
	}
	wg.Wait()
}

func execRedisCmd(rdb *redis.Client) {
	for i := 0; i < 250; i++ {
		var content Content
		if err := rdb.HMGet(ctx, "feeds-friends:"+strconv.Itoa(i), "post", "comments", "realmojis", "updatedAt").Scan(&content); err != nil {
			panic(err)
		}
	}
}

func main() {
	rdb := redisConnection()

	e := echo.New()

	// Middleware
	//e.Use(middleware.Logger())
	//e.Use(middleware.Recover())

	// Routes
	e.GET("/ok", getOk)
	e.GET("/redis-sync", getRedisSync(rdb))
	e.GET("/redis-async", getRedisAsync(rdb))

	// Start server
	e.Logger.Fatal(e.Start(":8080"))

}
