package com.cyfonly.thriftj.loadbalance;

/**
 * Create by pfliu on 2021/07/05.
 */
public class DefaultHashLoadBalancer extends AbstractLoadBalancer {

    protected DefaultHashLoadBalancer(String servers) {
        super(servers);
    }

    @Override
    public String getName() {
        return "defaultHash";
    }
}
