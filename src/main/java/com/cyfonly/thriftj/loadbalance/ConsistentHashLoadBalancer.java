package com.cyfonly.thriftj.loadbalance;

import com.cyfonly.thriftj.pool.ThriftServer;
import com.cyfonly.thriftj.utils.MurmurHash3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Create by pfliu on 2021/07/05.
 */
public class ConsistentHashLoadBalancer extends AbstractLoadBalancer {

    private static final Logger logger = LoggerFactory.getLogger(ConsistentHashLoadBalancer.class);

    // 用跳表模拟一致性hash环,即使在节点很多的情况下,也可以有不错的性能
    private final ConcurrentSkipListMap<Integer, ThriftServer> circle;
    // 虚拟节点数量
    private final int virtualSize;

    protected ConsistentHashLoadBalancer(String servers) {
        super(servers);
        this.circle = new ConcurrentSkipListMap<>();

        this.virtualSize = getVirtualSize(this.thriftServers.size());
        for (ThriftServer ts : this.thriftServers) {
            this.add(ts);
        }
    }

    @Override
    public ThriftServer find(byte[] key) {
        int hash = getHash(key);
        ConcurrentNavigableMap<Integer, ThriftServer> tailMap = circle.tailMap(hash);
        ThriftServer ts = tailMap.isEmpty() ? circle.firstEntry().getValue() : tailMap.firstEntry().getValue();
        // 注意,由于使用了虚拟节点,所以这里要做 虚拟节点 -> 真实节点的映射
        hit(ts);
        logger.info("ConsistentHash {} to {}", key, ts.getKey());
        return ts;
    }


    @Override
    public String getName() {
        return "ConsistentHash";
    }

    /**
     * 将每个节点添加进环中,并且添加对应数量的虚拟节点
     */
    private void add(ThriftServer c) {
        if (c == null) return;
        for (int i = 0; i < virtualSize; ++i) {
            String virtual = c.getKey() + "-v" + i;
            int hash = getHash(virtual);
            circle.put(hash, c);
        }
    }

    // 根据字符串获取hash值,这里使用简单粗暴的绝对值.
    private int getHash(String s) {
        return MurmurHash3.murmurhash3_x86_32(s, 0, s.length(), 0x1234ABCD);
    }

    private int getHash(byte[] key) {
        return MurmurHash3.murmurhash3_x86_32(key, 0, key.length, 0x1234ABCD);
    }

    // 计算当前需要多少个虚拟节点,这里没有计算,直接使用了150.
    private int getVirtualSize(int length) {
        return 150;
    }
}
