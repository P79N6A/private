package com.yurun.test.jython;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Objects;
import org.apache.commons.codec.CharEncoding;
import org.python.core.PyFunction;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.util.PythonInterpreter;

/**
 * Jython example.
 *
 * @author yurun
 */
public class JythonExample {
  private static String getScript() throws Exception {
    BufferedReader reader = null;

    try {
      reader =
          new BufferedReader(
              new InputStreamReader(
                  JythonExample.class.getClassLoader().getResourceAsStream("script"),
                  CharEncoding.UTF_8));

      StringBuilder sb = new StringBuilder();

      String line;

      while ((line = reader.readLine()) != null) {
        sb.append(line).append("\n");
      }

      return sb.toString();
    } finally {
      if (Objects.nonNull(reader)) {
        reader.close();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    PythonInterpreter interpreter = new PythonInterpreter();

    interpreter.exec(getScript());

    PyFunction function = interpreter.get("hello", PyFunction.class);

    PyObject object = function.__call__(new PyString("hello yurun"));

    Iterator<PyObject> iter = object.asIterable().iterator();

    while (iter.hasNext()) {
      String val = iter.next().asString();

      System.out.println(val);
    }

    interpreter.close();
  }
}
