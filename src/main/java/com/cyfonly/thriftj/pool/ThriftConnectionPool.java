package com.cyfonly.thriftj.pool;

import org.apache.thrift.TServiceClient;


/**
 * Thrift 连接池抽象接口
 *
 * @author yunfeng.cheng
 * @create 2016-11-18
 */
public interface ThriftConnectionPool<X extends TServiceClient> {

    X getClient(ThriftServer thriftServer);

    void returnClient(ThriftServer thriftServer, X x);

    void returnBrokenClient(ThriftServer thriftServer, X x);

    void close();

    void clear(ThriftServer thriftServer);

}
