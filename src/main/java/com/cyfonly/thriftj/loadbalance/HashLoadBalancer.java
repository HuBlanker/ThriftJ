package com.cyfonly.thriftj.loadbalance;

import com.cyfonly.thriftj.pool.ThriftServer;

public interface HashLoadBalancer extends LoadBalancer {

    ThriftServer find(byte[] key);

}
