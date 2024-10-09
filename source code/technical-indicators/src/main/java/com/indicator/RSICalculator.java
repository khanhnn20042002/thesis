package com.indicator;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RSICalculator extends IndicatorCalculator {
    // Relative Strength Index
    private int period;
    private String seriesType;

    public RSICalculator(String seriesType, int period) {
        this.period = period;
        this.seriesType = seriesType;
    }

    public RSICalculator() {
        this.period = 14;
        this.seriesType = SeriesType.CLOSE;
    }

    public double calculate(double ua, double da) {
        // ua: upward change average
        // da: downward change average
        return 100 - 100 / (1 + ua / da);
    }

    public double uaCalculate(int series, int previousSeries, double previousUA) {
        int u;
        if (series > previousSeries) {
            u = series - previousSeries;
        } else {
            u = 0;
        }
        return u + previousUA * (double) (this.period - 1) / this.period;
    }

    public double daCalculate(int series, int previousSeries, double previousDA) {
        int d;
        if (series < previousSeries) {
            d = previousSeries - series;
        } else {
            d = 0;
        }

        return d + previousDA * (double) (this.period - 1) / this.period;
    }

    @Override
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stockPrices = builder.stream("StockPrices");
        ObjectMapper objectMapper = new ObjectMapper();
        Utils utils = new Utils();

        Map<String, Integer[]> seriesMap = new HashMap<>();
        Map<String, Double[]> daMap = new HashMap<>();
        Map<String, Double[]> uaMap = new HashMap<>();

        KStream<String, String> rsiStream = stockPrices.mapValues((ValueMapper<String, String>) value -> {
            try {
                JsonNode jsonNode = objectMapper.readTree(value);

                String time = jsonNode.get("time").asText();
                String ticker = jsonNode.get("ticker").asText();

                Integer[] series = utils.getOrDefaultSeries(seriesMap, ticker, 2);
                utils.updateSeries(series, jsonNode.get(this.seriesType).asInt());
                seriesMap.put(ticker, series);

                Double[] ua = utils.getOrDefaultIndicator(uaMap, ticker, 2);
                double currentUA = uaCalculate(series[0], series[1], ua[1]);
                utils.updateIndicator(ua, currentUA);
                uaMap.put(ticker, ua);

                Double[] da = utils.getOrDefaultIndicator(daMap, ticker, 2);
                double currentDA = daCalculate(series[0], series[1], da[1]);
                utils.updateIndicator(da, currentDA);
                daMap.put(ticker, da);

                double currentRSI = calculate(currentUA, currentDA);

                Map<String, Object> result = new HashMap<>();
                result.put("time", time);
                result.put("ticker", ticker);
                result.put("rsi", currentRSI);

                return objectMapper.writeValueAsString(result);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        });

        rsiStream.to("RSI", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "RSICalculator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        RSICalculator rsiCalculator = new RSICalculator();
        KafkaStreams streams = new KafkaStreams(rsiCalculator.createTopology(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
