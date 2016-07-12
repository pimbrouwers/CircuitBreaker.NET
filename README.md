# CircuitBreaker.NET
Implementation of Circuit Breaker Pattern for .NET with Memory &amp; File System Caching - Access volatile resources in a thread safe manner.

## Usage
```c#
CircuitBreaker breaker = new CircuitBreaker("CircuitKey", "CacheKey");

var myResults = breaker.Execute(() =>
{
    return "hello world!";
});
```

## Constructor Options
```c#
string circuitId,
string cacheKey = null,
CacheDependency cacheDependency = null,
TimeSpan? cacheDuration = null,
TimeSpan? cacheSlidingExpiration = null,
CacheItemPriority cacheItemPriority = CacheItemPriority.Normal,
string workingDirectory = null,
int failureThreshold = 3,
TimeSpan? openTimeout = null
```