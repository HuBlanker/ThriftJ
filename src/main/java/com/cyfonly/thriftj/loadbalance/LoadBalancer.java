package com.cyfonly.thriftj.loadbalance;

import com.cyfonly.thriftj.pool.ThriftServer;

public interface LoadBalancer {

    ThriftServer find();

    void printStat();

    String getName();
}
