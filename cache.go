package limiter

import lru "github.com/hashicorp/golang-lru/v2"

var defaultCache, _ = lru.New[string, interface{}](1000)
