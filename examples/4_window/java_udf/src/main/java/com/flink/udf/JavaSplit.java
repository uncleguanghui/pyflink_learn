package com.flink.udf;

import org.apache.flink.table.functions.TableFunction;

import java.util.Arrays;

public class JavaSplit extends TableFunction<String> {
  public void eval(String str) {
    // use collect(...) to emit a row.
    // str.split("#").foreach(x => collect((x, x.length)))
    Arrays.stream(str.split("#")).forEach(x -> {
      collect(x);
    });
  }
}
