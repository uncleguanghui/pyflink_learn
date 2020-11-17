package com.flink.udf;

import java.util.HashMap;

public class TopNAccum {
    public HashMap<String, Integer> dictCount = new HashMap<>();
    public Integer N = 10; // 默认 10
}
