# Full Benchmark Matrix

| Test | Engine | Threads | RPS | Avg Latency (ms) | p99 Latency (ms) |
|---|---|---|---|---|---|
| PING_INLINE | Valkey_Shard | 4 | 66533.60 | 1.299 | 5.567 |
| PING_MBULK | Valkey_Shard | 4 | 66577.90 | 1.259 | 4.431 |
| SET | Valkey_Shard | 4 | 132978.73 | 0.519 | 1.191 |
| GET | Valkey_Shard | 4 | 133155.80 | 0.606 | 1.463 |
| INCR | Valkey_Shard | 4 | 132978.73 | 0.498 | 1.151 |
| LPUSH | Valkey_Shard | 4 | 132978.73 | 0.481 | 1.111 |
| RPUSH | Valkey_Shard | 4 | 133155.80 | 0.624 | 1.783 |
| LPOP | Valkey_Shard | 4 | 133155.80 | 0.519 | 1.199 |
| RPOP | Valkey_Shard | 4 | 132978.73 | 0.532 | 1.319 |
| SADD | Valkey_Shard | 4 | 99900.09 | 0.605 | 1.463 |
| HSET | Valkey_Shard | 4 | 132978.73 | 0.558 | 1.423 |
| SPOP | Valkey_Shard | 4 | 133155.80 | 0.484 | 1.215 |
| ZADD | Valkey_Shard | 4 | 133155.80 | 0.491 | 1.231 |
| ZPOPMIN | Valkey_Shard | 4 | 133155.80 | 0.521 | 1.327 |
| LPUSH (needed to benchmark LRANGE) | Valkey_Shard | 4 | 133155.80 | 0.471 | 1.039 |
| LRANGE_100 (first 100 elements) | Valkey_Shard | 4 | 79872.20 | 0.806 | 1.919 |
| LRANGE_300 (first 300 elements) | Valkey_Shard | 4 | 57045.07 | 1.137 | 2.695 |
| LRANGE_500 (first 500 elements) | Valkey_Shard | 4 | 39904.23 | 1.725 | 4.087 |
| LRANGE_600 (first 600 elements) | Valkey_Shard | 4 | 33244.68 | 1.963 | 4.527 |
| MSET (10 keys) | Valkey_Shard | 4 | 99800.40 | 0.850 | 1.783 |
| MGET (10 keys) | Valkey_Shard | 4 | 99900.09 | 0.653 | 1.703 |
| XADD | Valkey_Shard | 4 | 99800.40 | 0.647 | 1.591 |
| FUNCTION LOAD | Valkey_Shard | 4 | 12874.98 | 7.605 | 22.943 |
| PING_INLINE | Valkey_IO | 4 | 99800.40 | 0.849 | 2.007 |
| PING_MBULK | Valkey_IO | 4 | 133155.80 | 0.565 | 0.871 |
| SET | Valkey_IO | 4 | 132978.73 | 0.549 | 0.751 |
| GET | Valkey_IO | 4 | 133155.80 | 0.572 | 0.751 |
| INCR | Valkey_IO | 4 | 132978.73 | 0.572 | 0.855 |
| LPUSH | Valkey_IO | 4 | 133155.80 | 0.556 | 0.759 |
| RPUSH | Valkey_IO | 4 | 133155.80 | 0.532 | 0.759 |
| LPOP | Valkey_IO | 4 | 133155.80 | 0.563 | 0.695 |
| RPOP | Valkey_IO | 4 | 133155.80 | 0.522 | 0.679 |
| SADD | Valkey_IO | 4 | 133155.80 | 0.595 | 0.791 |
| HSET | Valkey_IO | 4 | 132978.73 | 0.588 | 0.823 |
| SPOP | Valkey_IO | 4 | 133155.80 | 0.594 | 0.807 |
| ZADD | Valkey_IO | 4 | 133155.80 | 0.585 | 0.791 |
| ZPOPMIN | Valkey_IO | 4 | 133155.80 | 0.544 | 0.759 |
| LPUSH (needed to benchmark LRANGE) | Valkey_IO | 4 | 132978.73 | 0.565 | 0.727 |
| LRANGE_100 (first 100 elements) | Valkey_IO | 4 | 132978.73 | 0.412 | 0.719 |
| LRANGE_300 (first 300 elements) | Valkey_IO | 4 | 66577.90 | 1.102 | 1.951 |
| LRANGE_500 (first 500 elements) | Valkey_IO | 4 | 49950.05 | 1.695 | 2.919 |
| LRANGE_600 (first 600 elements) | Valkey_IO | 4 | 44385.27 | 1.953 | 2.887 |
| MSET (10 keys) | Valkey_IO | 4 | 99900.09 | 0.688 | 1.055 |
| MGET (10 keys) | Valkey_IO | 4 | 133155.80 | 0.604 | 0.839 |
| XADD | Valkey_IO | 4 | 133155.80 | 0.574 | 0.767 |
| FUNCTION LOAD | Valkey_IO | 4 | 33288.95 | 2.890 | 3.759 |
| FCALL | Valkey_IO | 4 | 132978.73 | 0.574 | 0.751 |
| PING_INLINE | Dragonfly | 4 | 132978.73 | 0.324 | 0.455 |
| PING_MBULK | Dragonfly | 4 | 133155.80 | 0.357 | 0.687 |
| SET | Dragonfly | 4 | 132978.73 | 0.403 | 0.831 |
| GET | Dragonfly | 4 | 133155.80 | 0.390 | 0.703 |
| INCR | Dragonfly | 4 | 132978.73 | 0.353 | 0.527 |
| LPUSH | Dragonfly | 4 | 132978.73 | 0.364 | 0.567 |
| RPUSH | Dragonfly | 4 | 132978.73 | 0.367 | 0.615 |
| LPOP | Dragonfly | 4 | 132978.73 | 0.396 | 1.055 |
| RPOP | Dragonfly | 4 | 132978.73 | 0.383 | 0.847 |
| SADD | Dragonfly | 4 | 132978.73 | 0.358 | 0.743 |
| HSET | Dragonfly | 4 | 132978.73 | 0.382 | 1.015 |
| SPOP | Dragonfly | 4 | 133155.80 | 0.363 | 1.199 |
| ZADD | Dragonfly | 4 | 132978.73 | 0.391 | 0.847 |
| ZPOPMIN | Dragonfly | 4 | 133155.80 | 0.372 | 0.727 |
| LPUSH (needed to benchmark LRANGE) | Dragonfly | 4 | 133155.80 | 0.361 | 0.959 |
| LRANGE_100 (first 100 elements) | Dragonfly | 4 | 99800.40 | 0.621 | 3.527 |
| LRANGE_300 (first 300 elements) | Dragonfly | 4 | 66577.90 | 1.122 | 8.903 |
| LRANGE_500 (first 500 elements) | Dragonfly | 4 | 33277.87 | 2.388 | 15.319 |
| LRANGE_600 (first 600 elements) | Dragonfly | 4 | 39920.16 | 1.913 | 14.727 |
| MSET (10 keys) | Dragonfly | 4 | 132978.73 | 0.445 | 0.919 |
| MGET (10 keys) | Dragonfly | 4 | 133155.80 | 0.381 | 0.631 |
| XADD | Dragonfly | 4 | 133155.80 | 0.360 | 0.559 |
| PING_INLINE | Valkey_Shard | 8 | 99700.90 | 0.890 | 1.871 |
| PING_MBULK | Valkey_Shard | 8 | 99900.09 | 0.744 | 1.471 |
| SET | Valkey_Shard | 8 | 133155.80 | 0.578 | 1.119 |
| GET | Valkey_Shard | 8 | 133155.80 | 0.573 | 1.215 |
| INCR | Valkey_Shard | 8 | 132978.73 | 0.512 | 1.191 |
| LPUSH | Valkey_Shard | 8 | 133155.80 | 0.551 | 1.223 |
| RPUSH | Valkey_Shard | 8 | 132978.73 | 0.495 | 0.959 |
| LPOP | Valkey_Shard | 8 | 133155.80 | 0.528 | 1.007 |
| RPOP | Valkey_Shard | 8 | 133155.80 | 0.549 | 1.063 |
| SADD | Valkey_Shard | 8 | 132978.73 | 0.518 | 1.191 |
| HSET | Valkey_Shard | 8 | 132978.73 | 0.530 | 1.087 |
| SPOP | Valkey_Shard | 8 | 132978.73 | 0.472 | 1.007 |
| ZADD | Valkey_Shard | 8 | 132802.12 | 0.465 | 0.951 |
| ZPOPMIN | Valkey_Shard | 8 | 132978.73 | 0.509 | 1.375 |
| LPUSH (needed to benchmark LRANGE) | Valkey_Shard | 8 | 133155.80 | 0.538 | 1.127 |
| LRANGE_100 (first 100 elements) | Valkey_Shard | 8 | 99900.09 | 0.742 | 1.399 |
| LRANGE_300 (first 300 elements) | Valkey_Shard | 8 | 57045.07 | 1.368 | 2.391 |
| LRANGE_500 (first 500 elements) | Valkey_Shard | 8 | 39888.31 | 1.897 | 3.439 |
| LRANGE_600 (first 600 elements) | Valkey_Shard | 8 | 33233.63 | 2.225 | 3.743 |
| MSET (10 keys) | Valkey_Shard | 8 | 99800.40 | 0.672 | 1.151 |
| MGET (10 keys) | Valkey_Shard | 8 | 133155.80 | 0.590 | 1.063 |
| XADD | Valkey_Shard | 8 | 133155.80 | 0.615 | 1.231 |
| FUNCTION LOAD | Valkey_Shard | 8 | 10784.00 | 8.247 | 14.951 |
| PING_INLINE | Valkey_IO | 8 | 99601.60 | 0.883 | 1.903 |
| PING_MBULK | Valkey_IO | 8 | 199600.80 | 0.248 | 0.471 |
| SET | Valkey_IO | 8 | 199600.80 | 0.218 | 0.559 |
| GET | Valkey_IO | 8 | 199600.80 | 0.204 | 0.335 |
| INCR | Valkey_IO | 8 | 199600.80 | 0.201 | 0.647 |
| LPUSH | Valkey_IO | 8 | 199600.80 | 0.195 | 0.319 |
| RPUSH | Valkey_IO | 8 | 199203.20 | 0.198 | 0.471 |
| LPOP | Valkey_IO | 8 | 199600.80 | 0.195 | 0.335 |
| RPOP | Valkey_IO | 8 | 199203.20 | 0.187 | 0.303 |
| SADD | Valkey_IO | 8 | 199600.80 | 0.209 | 0.351 |
| HSET | Valkey_IO | 8 | 199600.80 | 0.188 | 0.319 |
| SPOP | Valkey_IO | 8 | 199600.80 | 0.183 | 0.455 |
| ZADD | Valkey_IO | 8 | 199600.80 | 0.187 | 0.319 |
| ZPOPMIN | Valkey_IO | 8 | 199600.80 | 0.194 | 0.567 |
| LPUSH (needed to benchmark LRANGE) | Valkey_IO | 8 | 199600.80 | 0.183 | 0.271 |
| LRANGE_100 (first 100 elements) | Valkey_IO | 8 | 133155.80 | 0.639 | 1.031 |
| LRANGE_300 (first 300 elements) | Valkey_IO | 8 | 57045.07 | 1.533 | 2.111 |
| LRANGE_500 (first 500 elements) | Valkey_IO | 8 | 36297.64 | 2.390 | 3.791 |
| LRANGE_600 (first 600 elements) | Valkey_IO | 8 | 33266.80 | 2.722 | 3.919 |
| MSET (10 keys) | Valkey_IO | 8 | 199600.80 | 0.420 | 0.575 |
| MGET (10 keys) | Valkey_IO | 8 | 199600.80 | 0.272 | 0.815 |
| XADD | Valkey_IO | 8 | 199600.80 | 0.184 | 0.287 |
| FUNCTION LOAD | Valkey_IO | 8 | 30712.53 | 3.030 | 4.823 |
| FCALL | Valkey_IO | 8 | 133155.80 | 0.544 | 1.039 |
| PING_INLINE | Dragonfly | 8 | 198412.69 | 0.211 | 0.327 |
| PING_MBULK | Dragonfly | 8 | 199600.80 | 0.237 | 0.719 |
| SET | Dragonfly | 8 | 199600.80 | 0.314 | 0.655 |
| GET | Dragonfly | 8 | 199600.80 | 0.299 | 1.199 |
| INCR | Dragonfly | 8 | 199600.80 | 0.295 | 0.815 |
| LPUSH | Dragonfly | 8 | 199600.80 | 0.359 | 1.311 |
| RPUSH | Dragonfly | 8 | 199600.80 | 0.349 | 1.255 |
| LPOP | Dragonfly | 8 | 199600.80 | 0.285 | 1.063 |
| RPOP | Dragonfly | 8 | 199203.20 | 0.323 | 1.095 |
| SADD | Dragonfly | 8 | 199600.80 | 0.299 | 0.583 |
| HSET | Dragonfly | 8 | 199203.20 | 0.326 | 1.047 |
| SPOP | Dragonfly | 8 | 199203.20 | 0.277 | 0.455 |
| ZADD | Dragonfly | 8 | 199600.80 | 0.312 | 1.207 |
| ZPOPMIN | Dragonfly | 8 | 199600.80 | 0.277 | 0.503 |
| LPUSH (needed to benchmark LRANGE) | Dragonfly | 8 | 199600.80 | 0.278 | 0.903 |
| LRANGE_100 (first 100 elements) | Dragonfly | 8 | 99800.40 | 0.856 | 4.463 |
| LRANGE_300 (first 300 elements) | Dragonfly | 8 | 49875.31 | 1.711 | 6.119 |
| LRANGE_500 (first 500 elements) | Dragonfly | 8 | 33211.56 | 2.538 | 8.863 |
| LRANGE_600 (first 600 elements) | Dragonfly | 8 | 28514.40 | 2.883 | 9.503 |
| MSET (10 keys) | Dragonfly | 8 | 199600.80 | 0.410 | 1.535 |
| MGET (10 keys) | Dragonfly | 8 | 199203.20 | 0.367 | 1.607 |
| XADD | Dragonfly | 8 | 198807.16 | 0.349 | 1.039 |
| PING_INLINE | Valkey_Shard | 16 | 132450.33 | 0.570 | 0.807 |
| PING_MBULK | Valkey_Shard | 16 | 132978.73 | 0.503 | 1.247 |
| SET | Valkey_Shard | 16 | 133155.80 | 0.544 | 1.303 |
| GET | Valkey_Shard | 16 | 132978.73 | 0.467 | 1.007 |
| INCR | Valkey_Shard | 16 | 132978.73 | 0.486 | 1.095 |
| LPUSH | Valkey_Shard | 16 | 132978.73 | 0.485 | 1.007 |
| RPUSH | Valkey_Shard | 16 | 199203.20 | 0.416 | 0.767 |
| LPOP | Valkey_Shard | 16 | 133155.80 | 0.471 | 1.007 |
| RPOP | Valkey_Shard | 16 | 199203.20 | 0.416 | 0.783 |
| SADD | Valkey_Shard | 16 | 132978.73 | 0.442 | 1.327 |
| HSET | Valkey_Shard | 16 | 132978.73 | 0.577 | 1.783 |
| SPOP | Valkey_Shard | 16 | 199203.20 | 0.428 | 1.359 |
| ZADD | Valkey_Shard | 16 | 132978.73 | 0.516 | 1.159 |
| ZPOPMIN | Valkey_Shard | 16 | 133155.80 | 0.464 | 0.887 |
| LPUSH (needed to benchmark LRANGE) | Valkey_Shard | 16 | 132802.12 | 0.534 | 1.479 |
| LRANGE_100 (first 100 elements) | Valkey_Shard | 16 | 99800.40 | 0.712 | 1.479 |
| LRANGE_300 (first 300 elements) | Valkey_Shard | 16 | 57012.54 | 1.326 | 2.567 |
| LRANGE_500 (first 500 elements) | Valkey_Shard | 16 | 44306.60 | 1.881 | 3.175 |
| LRANGE_600 (first 600 elements) | Valkey_Shard | 16 | 36258.16 | 2.249 | 4.039 |
| MSET (10 keys) | Valkey_Shard | 16 | 99800.40 | 0.692 | 1.623 |
| MGET (10 keys) | Valkey_Shard | 16 | 132978.73 | 0.502 | 1.015 |
| XADD | Valkey_Shard | 16 | 132978.73 | 0.550 | 1.455 |
| FUNCTION LOAD | Valkey_Shard | 16 | 9728.57 | 9.601 | 18.879 |
| PING_INLINE | Valkey_IO | 16 | 99502.48 | 0.913 | 2.303 |
| PING_MBULK | Valkey_IO | 16 | 199600.80 | 0.348 | 0.831 |
| SET | Valkey_IO | 16 | 199600.80 | 0.233 | 0.583 |
| GET | Valkey_IO | 16 | 396825.38 | 0.185 | 0.687 |
| INCR | Valkey_IO | 16 | 398406.41 | 0.131 | 0.207 |
| LPUSH | Valkey_IO | 16 | 198412.69 | 0.214 | 0.719 |
| RPUSH | Valkey_IO | 16 | 396825.38 | 0.152 | 0.303 |
| LPOP | Valkey_IO | 16 | 398406.41 | 0.169 | 0.399 |
| RPOP | Valkey_IO | 16 | 396825.38 | 0.170 | 0.335 |
| SADD | Valkey_IO | 16 | 396825.38 | 0.158 | 0.815 |
| HSET | Valkey_IO | 16 | 396825.38 | 0.150 | 0.303 |
| SPOP | Valkey_IO | 16 | 396825.38 | 0.153 | 0.367 |
| ZADD | Valkey_IO | 16 | 398406.41 | 0.164 | 0.751 |
| ZPOPMIN | Valkey_IO | 16 | 396825.38 | 0.153 | 0.335 |
| LPUSH (needed to benchmark LRANGE) | Valkey_IO | 16 | 396825.38 | 0.141 | 0.263 |
| LRANGE_100 (first 100 elements) | Valkey_IO | 16 | 132978.73 | 0.561 | 1.199 |
| LRANGE_300 (first 300 elements) | Valkey_IO | 16 | 57045.07 | 1.470 | 3.863 |
| LRANGE_500 (first 500 elements) | Valkey_IO | 16 | 36271.31 | 2.475 | 5.375 |
| LRANGE_600 (first 600 elements) | Valkey_IO | 16 | 36258.16 | 2.627 | 3.703 |
| MSET (10 keys) | Valkey_IO | 16 | 132978.73 | 0.578 | 1.007 |
| MGET (10 keys) | Valkey_IO | 16 | 199600.80 | 0.309 | 0.783 |
| XADD | Valkey_IO | 16 | 199203.20 | 0.218 | 0.455 |
| FUNCTION LOAD | Valkey_IO | 16 | 30684.26 | 3.021 | 7.231 |
| FCALL | Valkey_IO | 16 | 199203.20 | 0.317 | 0.423 |
| PING_INLINE | Dragonfly | 16 | 390624.97 | 0.155 | 0.879 |
| PING_MBULK | Dragonfly | 16 | 396825.38 | 0.148 | 0.671 |
| SET | Dragonfly | 16 | 199203.20 | 0.252 | 0.935 |
| GET | Dragonfly | 16 | 199600.80 | 0.222 | 1.207 |
| INCR | Dragonfly | 16 | 199203.20 | 0.208 | 0.967 |
| LPUSH | Dragonfly | 16 | 199600.80 | 0.301 | 1.015 |
| RPUSH | Dragonfly | 16 | 199203.20 | 0.294 | 1.311 |
| LPOP | Dragonfly | 16 | 198019.80 | 0.238 | 1.103 |
| RPOP | Dragonfly | 16 | 199203.20 | 0.263 | 1.463 |
| SADD | Dragonfly | 16 | 199203.20 | 0.247 | 0.951 |
| HSET | Dragonfly | 16 | 199600.80 | 0.249 | 0.967 |
| SPOP | Dragonfly | 16 | 199203.20 | 0.236 | 0.927 |
| ZADD | Dragonfly | 16 | 199203.20 | 0.255 | 0.871 |
| ZPOPMIN | Dragonfly | 16 | 199600.80 | 0.213 | 1.071 |
| LPUSH (needed to benchmark LRANGE) | Dragonfly | 16 | 199203.20 | 0.240 | 0.943 |
| LRANGE_100 (first 100 elements) | Dragonfly | 16 | 99700.90 | 0.768 | 2.351 |
| LRANGE_300 (first 300 elements) | Dragonfly | 16 | 49875.31 | 1.651 | 4.191 |
| LRANGE_500 (first 500 elements) | Dragonfly | 16 | 33244.68 | 2.650 | 6.719 |
| LRANGE_600 (first 600 elements) | Dragonfly | 16 | 30674.85 | 2.882 | 7.151 |
| MSET (10 keys) | Dragonfly | 16 | 199203.20 | 0.420 | 2.159 |
| MGET (10 keys) | Dragonfly | 16 | 199600.80 | 0.392 | 2.175 |
| XADD | Dragonfly | 16 | 199600.80 | 0.291 | 0.903 |
