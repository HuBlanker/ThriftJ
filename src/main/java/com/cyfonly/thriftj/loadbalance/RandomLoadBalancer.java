package com.cyfonly.thriftj.loadbalance;

import com.cyfonly.thriftj.pool.ThriftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Create by pfliu on 2021/07/05.
 */
public class RandomLoadBalancer extends AbstractLoadBalancer {
    private static final Logger logger = LoggerFactory.getLogger(RandomLoadBalancer.class);


    protected RandomLoadBalancer(String servers) {
        super(servers);
    }

    @Override
    public ThriftServer find() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        int i = r.nextInt(this.thriftServers.size());
        ThriftServer ts = this.thriftServers.get(i);
        logger.info("LoadBalancer: Random. {} from {} servers.", i, this.thriftServers.size());
        hit(ts);
        return ts;
    }

    @Override
    public String getName() {
        return "Random";
    }
}
