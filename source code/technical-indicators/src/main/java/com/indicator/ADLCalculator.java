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

public class ADLCalculator extends IndicatorCalculator {
    // Accumulation/Distribution Line
    public ADLCalculator() {
        // empty
    }

    public double calculate(int close, int low, int high, int volume, double previousADL) {
        double moneyFlowMultiplier = (double) ((close - low) - (high - close)) / (high - low);
        double moneyFlowVolume = moneyFlowMultiplier * volume;
        return previousADL + moneyFlowVolume;

    }

    @Override
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stockPrices = builder.stream("StockPrices");
        ObjectMapper objectMapper = new ObjectMapper();
        Utils utils = new Utils();

        Map<String, Double[]> adlMap = new HashMap<>();
        Map<String, Integer[]> closeMap = new HashMap<>();
        Map<String, Integer[]> lowMap = new HashMap<>();
        Map<String, Integer[]> highMap = new HashMap<>();
        Map<String, Integer[]> volumeMap = new HashMap<>();

        KStream<String, String> adlStream = stockPrices.mapValues((ValueMapper<String, String>) value -> {
            try {
                JsonNode jsonNode = objectMapper.readTree(value);
                String time = jsonNode.get("time").asText();
                String ticker = jsonNode.get("ticker").asText();

                Integer[] close = utils.getOrDefaultSeries(closeMap, ticker, 1);
                utils.updateSeries(close, jsonNode.get(SeriesType.CLOSE).asInt());
                closeMap.put(ticker, close);

                Integer[] low = utils.getOrDefaultSeries(lowMap, ticker, 1);
                utils.updateSeries(low, jsonNode.get(SeriesType.LOW).asInt());
                lowMap.put(ticker, low);

                Integer[] high = utils.getOrDefaultSeries(highMap, ticker, 1);
                utils.updateSeries(high, jsonNode.get(SeriesType.HIGH).asInt());
                highMap.put(ticker, high);

                Integer[] volume = utils.getOrDefaultSeries(volumeMap, ticker, 1);
                utils.updateSeries(volume, jsonNode.get(SeriesType.VOLUME).asInt());
                volumeMap.put(ticker, volume);

                Double[] adl = utils.getOrDefaultIndicator(adlMap, ticker, 2);
                double currentADL = calculate(close[0], low[0], high[0], volume[0], adl[1]);
                utils.updateIndicator(adl, currentADL);
                adlMap.put(ticker, adl);

                Map<String, Object> result = new HashMap<>();
                result.put("time", time);
                result.put("ticker", ticker);
                result.put("adl", currentADL);

                return objectMapper.writeValueAsString(result);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        });

        adlStream.to("ADL", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ADLCalculator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ADLCalculator adlCalculator = new ADLCalculator();

        KafkaStreams streams = new KafkaStreams(adlCalculator.createTopology(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}