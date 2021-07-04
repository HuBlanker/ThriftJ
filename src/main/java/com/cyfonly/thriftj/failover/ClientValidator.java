package com.cyfonly.thriftj.failover;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransport;

/**
 * 连接验证
 * @author yunfeng.cheng
 * @create 2016-11-19
 */
public interface ClientValidator<X extends TServiceClient> {

	boolean isValid(X x);
	
}
