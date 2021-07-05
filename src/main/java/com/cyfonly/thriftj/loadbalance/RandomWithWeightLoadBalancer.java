package com.cyfonly.thriftj.loadbalance;

import com.cyfonly.thriftj.pool.ThriftServer;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Create by pfliu on 2021/07/05.
 */
public class RandomWithWeightLoadBalancer extends AbstractLoadBalancer {

    private final int weightTotal;

    protected RandomWithWeightLoadBalancer(String servers) {
        super(servers);
        this.weightTotal = this.thriftServers.stream().mapToInt(ThriftServer::getWeight).sum();
    }


    @Override
    public ThriftServer find() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        int i = r.nextInt(weightTotal);
        int idx = 0;

        for (ThriftServer ts : this.thriftServers) {
            idx += ts.getWeight();
            if (idx > i) {
                hit(ts);
                return ts;
            }
        }
        return this.thriftServers.get(0);
    }

    @Override
    public String getName() {
        return "RoundWithWeight";
    }
}
