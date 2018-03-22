package com.yurun.common;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 18/3/22.
 */
public class ProxyMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProxyMain.class);

  private interface Service {

    int abs(int a);

    int add(int a, int b);

  }

  private static class ServiceImpl implements Service {

    public int abs(int a) {
      return a;
    }

    public int add(int a, int b) {
      return abs(a) + b;
    }

  }

  private static class ServiceHandler implements InvocationHandler {

    private Object target;

    public ServiceHandler(Object target) {
      this.target = target;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      long begin = System.currentTimeMillis();

      Object result = method.invoke(target, args);

      long end = System.currentTimeMillis();

      long time = end - begin;

      LOGGER.info("method name: {}, consume time: {}", method.getName(), time);

      return result;
    }

  }

  public static void main(String[] args) {
    ServiceImpl serviceImpl = new ServiceImpl();

    ServiceHandler serviceHandler = new ServiceHandler(serviceImpl);

    Service service = (Service) Proxy.newProxyInstance(
        Service.class.getClassLoader(),
        ServiceImpl.class.getInterfaces(),
        serviceHandler);

    service.abs(1);
    service.abs(2);

    int result = service.add(1, 2);

    LOGGER.info("result: " + result);
  }

}
