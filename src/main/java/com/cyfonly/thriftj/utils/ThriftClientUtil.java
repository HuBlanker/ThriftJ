package com.cyfonly.thriftj.utils;

import com.google.common.collect.Sets;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.*;


/**
 * @author yunfeng.cheng
 * @create 2016-11-19
 */
public class ThriftClientUtil {

    private static ConcurrentMap<Class<?>, Set<String>> interfaceMethodCache = new ConcurrentHashMap<>();

    public static final int randomNextInt() {
        return ThreadLocalRandom.current().nextInt();
    }

    public static final Set<String> getInterfaceMethodNames(Class<?> ifaceClass) {
        if (interfaceMethodCache.containsKey(ifaceClass))
            return interfaceMethodCache.get(ifaceClass);
        Set<String> methodName = Sets.newHashSet();
        Class<?>[] interfaces = ifaceClass.getInterfaces();
        for (Class<?> class1 : interfaces) {
            Method[] methods = class1.getMethods();
            for (Method method : methods) {
                methodName.add(method.getName());
            }
        }
        interfaceMethodCache.putIfAbsent(ifaceClass, methodName);
        return methodName;
    }

}
