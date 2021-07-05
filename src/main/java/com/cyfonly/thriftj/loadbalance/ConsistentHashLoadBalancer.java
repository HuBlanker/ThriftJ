package com.cyfonly.thriftj.loadbalance;

import com.cyfonly.thriftj.pool.ThriftServer;

/**
 * Create by pfliu on 2021/07/05.
 */
public class ConsistentHashLoadBalancer extends AbstractLoadBalancer {
    protected ConsistentHashLoadBalancer(String servers) {
        super(servers);
    }

    @Override
    public ThriftServer find(byte[] key) {
        return null;
    }

    @Override
    public String getName() {
        return "ConsistentHash";
    }
}
