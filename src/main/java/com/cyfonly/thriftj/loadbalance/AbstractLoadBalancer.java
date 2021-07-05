package com.cyfonly.thriftj.loadbalance;

import com.cyfonly.thriftj.pool.ThriftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Create by pfliu on 2021/07/05.
 */
public abstract class AbstractLoadBalancer implements HashLoadBalancer {
    private static final Logger logger = LoggerFactory.getLogger(AbstractLoadBalancer.class);

    protected final String servers;
    protected final List<ThriftServer> thriftServers;
    private final Map<ThriftServer, AtomicLong> stat;


    protected AbstractLoadBalancer(String servers) {
        this.servers = servers;

        this.thriftServers = ThriftServer.parse(servers);
        this.stat = new ConcurrentHashMap<>();
    }

    @Override
    public ThriftServer find(byte[] key) {
        throw new RuntimeException("Abstract LoadBalancer not implement.");
    }

    @Override
    public ThriftServer find() {
        throw new RuntimeException("Abstract LoadBalancer not implement.");
    }

    @Override
    public void printStat() {
        StringBuilder sb = new StringBuilder(getName() + " Load Balancer:");
        int t = 0;
        for (Map.Entry<ThriftServer, AtomicLong> e : stat.entrySet()) {
            long c = e.getValue().get();
            sb.append(e.getKey().getKey()).append("->").append(c).append("\t");
            t += c;

        }
        sb.append("total").append("->").append(t);
        logger.info(sb.toString());
    }

    protected void hit(ThriftServer t) {
        this.stat.computeIfAbsent(t, i -> new AtomicLong(0)).incrementAndGet();
    }
}
