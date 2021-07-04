package com.cyfonly.thriftj.test.thriftclient;

import net.sf.cglib.proxy.*;

import java.lang.reflect.Method;

/**
 * Create by pfliu on 2021/07/04.
 */

public class DayProxy {


    public static interface TT {
        void tt();
    }

    public static class TT1 implements TT{

        @Override
        public void tt() {
            System.out.println("tt1");
        }
    }


    public static void main(String[] args) {

        TT tt = new TT() {

            @Override
            public void tt() {
                System.out.println("1");
            }
        };
        Enhancer en = new Enhancer();
        en.setSuperclass(TT1.class);
        en.setCallback(new MethodInterceptor() {
            @Override
            public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {

                System.out.println("before");

                Object o1 = methodProxy.invokeSuper(o, objects);
                System.out.println("end");
                return o1;
            }
        });

        TT aa = (TT) en.create();
        aa.tt();

    }
}
