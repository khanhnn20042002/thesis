package com.indicator;

import java.util.Arrays;
import java.util.Map;

public class Utils {
    public Integer[] getOrDefaultSeries(Map<String, Integer[]> map, String ticker, int lookBack) {
        Integer[] arr;
        if (map.containsKey(ticker)) {
            arr = map.get(ticker);
        } else {
            arr = new Integer[lookBack];
            Arrays.fill(arr, 0);
        }
        return arr;
    }

    public void updateSeries(Integer[] arr, int val) {
        for (int i = arr.length - 2; i >= 0; i--) {
            arr[i + 1] = arr[i];
        }
        arr[0] = val;
    }

    public Double[] getOrDefaultIndicator(Map<String, Double[]> map, String ticker, int lookBack) {
        Double[] arr;
        if (map.containsKey(ticker)) {
            arr = map.get(ticker);
        } else {
            arr = new Double[lookBack];
            Arrays.fill(arr, 0.0);
        }
        return arr;
    }

    public void updateIndicator(Double[] arr, double val) {
        for (int i = arr.length - 2; i >= 1; i--) {
            arr[i + 1] = arr[i];
        }
        arr[1] = val;
    }

}
