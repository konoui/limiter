### Publish Metrics

```
echo '{"_aws":{"CloudWatchMetrics":[{"Namespace":"RateLimit","Dimensions":[["TableName","BucketID","ShardID","Operation"]],"Metrics":[{"Name":"Throttle","Unit":"Count"}]}],"Timestamp":1675510655392},"TableName":"dummy_table","BucketID":"dummy","ShardID":"0","Operation":"GetItem","Throttle":1,"requestId":"1fd3caae-5e0f-45aa-b947-f7d2318c32d0"}' | ./cmd/cmd publish-metric --log-group-name emf-test --log-stream-name emf-test
```
