package com.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class DateTime extends ScalarFunction {
  public String eval(String str) {
    return new org.joda.time.DateTime().toString();
  }
}
