# Transactor
A library that will facilitate transactions with Redis and Postgres. It provides a simple way to manage transactions within your application.

## Installation
To use Transactor in your project, simply import it:

``` import "github.com/global-soft-ba/go-eventstore/transactor"```

and install it: 

```go get github.com/global-soft-ba/go-eventstore/transactor```


## Usage
One can use either postgres or redis transactor both works similarly but for different storage systems.

Creating a Redis Transactor
To create a new Transactor, you need to pass a Redis client instance. You can create a Transactor using the NewTransactor function:

```
redisClient := redis.NewClient(
// redis credentials
)

transactor := transactor.NewTransactor(redisClient)
```

Creating a Postgres Transactor
To create postgres transactor, just pass pointer to pgxpool.Pool connection object for postgres, from  "github.com/jackc/pgx/v4/pgxpool" lib:

```
//configure connection

pool, err := pgxpool.ConnectConfig(ctx, conf)
if err != nil {
    //
}

transactor := transactor.NewTransactor(pool)
```