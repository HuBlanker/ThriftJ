package com.cyfonly.thriftj.loadbalance;

import com.cyfonly.thriftj.pool.ThriftServer;
import com.cyfonly.thriftj.utils.MurmurHash3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create by pfliu on 2021/07/05.
 */
public class DefaultHashLoadBalancer extends AbstractLoadBalancer {

    private static final Logger logger = LoggerFactory.getLogger(DefaultHashLoadBalancer.class);

    protected DefaultHashLoadBalancer(String servers) {
        super(servers);
    }


    @Override
    public ThriftServer find(byte[] key) {
        int i = MurmurHash3.murmurhash3_x86_32(key, 0, key.length, 0x1234ABCD);
        int index = Math.abs(i) % this.thriftServers.size();
        ThriftServer r = this.thriftServers.get(index);
        hit(r);
        logger.info("DefaultHash {} to {} {}", key, index, r.getKey());
        return r;
    }

    @Override
    public String getName() {
        return "DefaultHash";
    }
}
