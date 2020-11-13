package com.flink.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * The top2 user-defined table aggregate function.
 */
public class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accum> {

  @Override
  public Top2Accum createAccumulator() {
    Top2Accum acc = new Top2Accum();
    acc.first = Integer.MIN_VALUE;
    acc.second = Integer.MIN_VALUE;
    return acc;
  }


  public void accumulate(Top2Accum acc, Integer v) {
    if (v > acc.first) {
      acc.second = acc.first;
      acc.first = v;
    } else if (v > acc.second) {
      acc.second = v;
    }
  }

  public void merge(Top2Accum acc, java.lang.Iterable<Top2Accum> iterable) {
    for (Top2Accum otherAcc : iterable) {
      accumulate(acc, otherAcc.first);
      accumulate(acc, otherAcc.second);
    }
  }

  public void emitValue(Top2Accum acc, Collector<Tuple2<Integer, Integer>> out) {
    // emit the value and rank
    if (acc.first != Integer.MIN_VALUE) {
      out.collect(Tuple2.of(acc.first, 1));
    }
    if (acc.second != Integer.MIN_VALUE) {
      out.collect(Tuple2.of(acc.second, 2));
    }
  }
}
