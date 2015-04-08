# flume-more
more sources, interceptors and others

## more sources
### com.github.bingoohuang.flume.more.source.ExecBlockSource
Enhance exec source to support a log event which may contain multiple lines like java exception trace stacks.
example:
    a1.sources = r1
    a1.channels = c1
    a1.sources.r1.type = com.github.bingoohuang.flume.more.source.ExecBlockSource
    a1.sources.r1.boundaryRegex=\d{4}-\d{2}-\d{2}
    a1.sources.r1.command = tail -F /var/log/secure
    a1.sources.r1.channels = c1

## more interceptors
### com.github.bingoohuang.flume.more.interceptor.MultiStaticInterceptor
Interceptor class that appends multiple static, pre-configured headers to all events.
example:
    agent.sources.r1.channels = c1
    agent.sources.r1.type = SEQ
    agent.sources.r1.interceptors = i1
    agent.sources.r1.interceptors.i1.type = com.github.bingoohuang.flume.more.interceptor.MultiStaticInterceptor
    agent.sources.r1.interceptors.i1.preserveExisting = false
    agent.sources.r1.interceptors.i1.keyValues = key1:value1, key2:value2