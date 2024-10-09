package com.indicator;

import java.util.Arrays;

public class Test {
    public static void main(String[] args) {
        int num_periods = 5; // Example value
        Integer[] defaultValue = new Integer[num_periods];
        Arrays.fill(defaultValue, 0);

        // Create a new array for historyHigh and copy the contents of defaultValue into
        // it
        Integer[] historyHigh = new Integer[num_periods];
        System.arraycopy(defaultValue, 0, historyHigh, 0, num_periods);

        // Now defaultValue and historyHigh point to different objects
        defaultValue[0] = 1;
        System.out.println("defaultValue[0]: " + defaultValue[0]); // This will print 1
        System.out.println("historyHigh[0]: " + historyHigh[0]); // This will print 0
    }
}
