package com.cyfonly.thriftj.loadbalance;

import com.cyfonly.thriftj.pool.ThriftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create by pfliu on 2021/07/05.
 */
public class LeastConnectionLoadBalancer extends AbstractLoadBalancer {

    private static final Logger logger = LoggerFactory.getLogger(LeastConnectionLoadBalancer.class);

    protected LeastConnectionLoadBalancer(String servers) {
        super(servers);
    }

    @Override
    public String getName() {
        return "LeastConnection";
    }

    @Override
    public ThriftServer find() {
        int minIdx = 0;
        int minGoing = Integer.MAX_VALUE;
        for (int i = 0; i < this.thriftServers.size(); i++) {
            int g = this.thriftServers.get(i).getGoing();
            if (g < minGoing) {
                minGoing = g;
                minIdx = i;
            }
        }
        ThriftServer r = this.thriftServers.get(minIdx);
        hit(r);
        logger.info("LeastConnection {} con {}, {} from {}", r.getKey(), minGoing, minIdx, this.thriftServers.size());
        return r;
    }
}
