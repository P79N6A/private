package com.yurun.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.Arrays;
import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.CallbackFilter;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import net.sf.cglib.proxy.NoOp;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CglibMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(CglibMain.class);

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface Profile {

  }

  private static class ProfileCallbackFilter implements CallbackFilter {

    public int accept(Method method) {
      if (method.isAnnotationPresent(Profile.class)) {
        return 0;
      }

      return 1;
    }

  }

  public static class HelloService {

    public String name() {
      return "yurun";
    }

    @Profile
    public void sayHello(String name) {
      if (StringUtils.isEmpty(name)) {
        name = name();
      }

      LOGGER.info("say {}", name);
    }

  }

  private static class MethodExecuteInterceptor implements MethodInterceptor {

    /*
      TODO:

      ApplicationName/IP/Container/Job/Task

      Method consume time: Sum/Max/Min/Avg/Count

      Method consume time overtime: log debug
     */
    public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy)
        throws Throwable {
      long begin = System.currentTimeMillis();

      Object result = methodProxy.invokeSuper(o, objects);

      long end = System.currentTimeMillis();

      LOGGER.info("method name: {}, parameters: {}, consume time: {}",
          method.getName(), Arrays.toString(objects), (end - begin));

      return result;
    }

  }

  @SuppressWarnings("unchecked")
  public static <T> T createProxy(Class<T> clazz) {
    Enhancer enhancer = new Enhancer();

    enhancer.setSuperclass(clazz);

    enhancer.setCallbackFilter(new ProfileCallbackFilter());
    enhancer.setCallbacks(new Callback[]{new MethodExecuteInterceptor(), NoOp.INSTANCE});

    return (T) enhancer.create();
  }

  public static void main(String[] args) {
    HelloService helloService = createProxy(HelloService.class);

    helloService.sayHello("dip");

    helloService.sayHello(null);
  }

}
