package r2cache_test

import (
	"context"
	"fmt"

	"github.com/kapitanov/r2cache"
)

func ExampleNewJSONSerializer() {
	type item struct {
		Name string `json:"name"`
	}

	serializer := r2cache.NewJSONSerializer[item]()
	data, _ := serializer.Marshal(item{Name: "example"})

	var value item
	_ = serializer.Unmarshal(data, &value)

	fmt.Println(string(data), value.Name)
	// Output: {"name":"example"} example
}

// No Output annotation: this example is compiled without requiring a live server.
func ExampleNewBuilder() {
	cache, err := r2cache.NewBuilder[string, string]().
		RedisURL("redis://localhost:6379/0").
		KeyPrefix("my-cache-v1").
		Fetch(func(ctx context.Context, key string) (string, error) { return "value for " + key, nil }).
		Build()
	if err != nil {
		panic(err)
	}

	defer cache.Shutdown()
	value, err := cache.GetOrFetch(context.Background(), "my-key")

	fmt.Println(value, err)
}
