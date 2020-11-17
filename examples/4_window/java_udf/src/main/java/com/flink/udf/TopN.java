package com.flink.udf;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * The topN user-defined table aggregate function.
 * https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/functions/udfs.html
 */

public class TopN extends AggregateFunction<String, TopNAccum> {


    @Override
    public TopNAccum createAccumulator() {  // 必须
        // 创建一个空的累加器，保存聚合中间结果
        return new TopNAccum();
    }

    @Override
    public String getValue(TopNAccum acc) {
        if (acc.dictCount.size() == 0) {
            return null;
        } else {
            // 对 Map 的 value 进行降序排序，并取前 N 个
            List<String> resultList = acc.dictCount.entrySet().stream()
                    .sorted((Entry.<String, Integer>comparingByValue().reversed())) // 逆序
                    // .sorted(Entry.comparingByValue()) // 正序
                    // .sorted((Entry<String, Integer> e1, Entry<String, Integer> e2) -> e2.getValue() - e1.getValue())
                    .map(entry -> "{\"" + entry.getKey() + "\":" + entry.getValue() + "}")
                    .collect(Collectors.toList())
                    .subList(0, Math.min(acc.N, acc.dictCount.size()));
            return String.join(",", resultList);
        }
    }

    public void accumulate(TopNAccum acc, String name, Integer N, Integer count) {  // 必须
        // 每个输入行调用函数的方法以更新累加器
        // acc 累加器（默认）
        // name 要统计出现次数的名称（应用函数时传入的值）
        // N 最终返回的 TopN 结果里的 N（应用函数时传入的值）
        // count 每次累加时要加的次数（应用函数时传入的值，应该传 1 ）
        acc.N = N;
        if (acc.dictCount.containsKey(name)) {
            acc.dictCount.put(name, acc.dictCount.get(name) + count);
        } else {
            acc.dictCount.put(name, count);
        }
    }

    public void retract(TopNAccum acc, String name, Integer N, Integer count) {
        // 撤回
        if (acc.dictCount.containsKey(name)) {
            if (acc.dictCount.get(name) > count) {
                acc.dictCount.put(name, acc.dictCount.get(name) - count);
            } else {
                acc.dictCount.remove(name);
            }
        }
    }

    public void merge(TopNAccum acc, java.lang.Iterable<TopNAccum> iterable) {
        // 合并两个累加器
        for (TopNAccum otherAcc : iterable) {
            // 遍历迭代器
            for (Entry<String, Integer> entry : otherAcc.dictCount.entrySet()) {
                // 遍历迭代器中的元素
                String name = entry.getKey();
                Integer count = entry.getValue();
                // 更新 acc 各 key 的数值
                accumulate(acc, name, acc.N, count);
            }
        }
    }

    public void resetAccumulator(TopNAccum acc) {
        // 重置累加器
        acc.dictCount.clear();
        acc.N = 0;
    }
}
