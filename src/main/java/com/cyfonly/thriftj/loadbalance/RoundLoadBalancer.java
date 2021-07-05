package com.cyfonly.thriftj.loadbalance;

import com.cyfonly.thriftj.pool.ThriftServer;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Create by pfliu on 2021/07/05.
 */
public class RoundLoadBalancer extends AbstractLoadBalancer {

    private final AtomicInteger i = new AtomicInteger(0);

    protected RoundLoadBalancer(String servers) {
        super(servers);
    }

    @Override
    public ThriftServer find() {
        ThriftServer r = this.thriftServers.get(i.get());
        incr();
        hit(r);
        return r;
    }

    public void incr() {
        if (this.thriftServers.size() == i.incrementAndGet()) {
            i.set(0);
        }
    }

    @Override
    public String getName() {
        return "Round";
    }
}
