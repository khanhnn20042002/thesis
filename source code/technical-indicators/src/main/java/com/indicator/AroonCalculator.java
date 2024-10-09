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

public class AroonCalculator extends IndicatorCalculator {
    private int period;

    public AroonCalculator(int period) {
        this.period = period;
    }

    public AroonCalculator() {
        this.period = 25;
    }

    public void setPeriod(int period) {
        this.period = period;
    }

    public int getPeriod() {
        return this.period;
    }

    public double aroonUpCalculate(Integer[] high) {
        int maxIdx = 0;
        int max = high[0];
        for (int i = 1; i < this.period; i++) {
            if (high[i] > max) {
                max = high[i];
                maxIdx = i;
            }
        }
        return (double) (this.period - maxIdx) / this.period * 100;
    }

    public double aroonDownCalculate(Integer[] low) {
        int minIdx = 0;
        int min = low[0];
        for (int i = 1; i < this.period; i++) {
            if (low[i] < min) {
                min = low[i];
                minIdx = i;
            }
        }
        return (double) (this.period - minIdx) / this.period * 100;
    }

    public double aroonOscillatorCalculate(double aroonUp, double aroonDown) {
        return aroonUp - aroonDown;
    }

    @Override
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stockPrices = builder.stream("StockPrices");

        ObjectMapper objectMapper = new ObjectMapper();
        Utils utils = new Utils();

        Map<String, Integer[]> highMap = new HashMap<>();
        Map<String, Integer[]> lowMap = new HashMap<>();
        KStream<String, String> aroonStream = stockPrices.mapValues((ValueMapper<String, String>) value -> {
            try {
                JsonNode jsonNode = objectMapper.readTree(value);
                String time = jsonNode.get("time").asText();
                String ticker = jsonNode.get("ticker").asText();

                Integer[] high = utils.getOrDefaultSeries(highMap, ticker, this.period);
                utils.updateSeries(high, jsonNode.get(SeriesType.HIGH).asInt());
                highMap.put(ticker, high);

                Integer[] low = utils.getOrDefaultSeries(lowMap, ticker, this.period);
                utils.updateSeries(low, jsonNode.get(SeriesType.LOW).asInt());
                lowMap.put(ticker, low);

                double currentAroonUp = aroonUpCalculate(high);
                double currentAroonDown = aroonDownCalculate(low);
                double currentAroonOscillator = aroonOscillatorCalculate(currentAroonUp, currentAroonDown);

                Map<String, Object> result = new HashMap<>();
                result.put("time", time);
                result.put("ticker", ticker);
                result.put("aroonUp", currentAroonUp);
                result.put("aroonDown", currentAroonDown);
                result.put("aroonOscillator", currentAroonOscillator);

                return objectMapper.writeValueAsString(result);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        });
        aroonStream.to("Aroon", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "AroonCalculator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        AroonCalculator aroonCalculator = new AroonCalculator();

        KafkaStreams streams = new KafkaStreams(aroonCalculator.createTopology(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
