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

public class OBVCalculator extends IndicatorCalculator {
    // On-Balance Volume

    public OBVCalculator() {
        // empty
    }

    public double calculate(int close, int previousClose, int volume, double previousOBV) {
        if (close > previousClose) {
            return previousOBV + volume;
        } else if (close < previousClose) {
            return previousOBV - volume;
        } else {
            return previousOBV;
        }
    }

    @Override
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stockPrices = builder.stream("StockPrices");

        ObjectMapper objectMapper = new ObjectMapper();
        Utils utils = new Utils();

        Map<String, Integer[]> closeMap = new HashMap<>();
        Map<String, Integer[]> volumeMap = new HashMap<>();
        Map<String, Double[]> obvMap = new HashMap<>();

        KStream<String, String> obvStream = stockPrices.mapValues((ValueMapper<String, String>) value -> {
            try {
                JsonNode jsonNode = objectMapper.readTree(value);
                String time = jsonNode.get("time").asText();
                String ticker = jsonNode.get("ticker").asText();

                Integer[] close = utils.getOrDefaultSeries(closeMap, ticker, 2);
                utils.updateSeries(close, jsonNode.get(SeriesType.CLOSE).asInt());
                closeMap.put(ticker, close);

                Integer[] volume = utils.getOrDefaultSeries(volumeMap, ticker, 1);
                utils.updateSeries(volume, jsonNode.get(SeriesType.VOLUME).asInt());
                volumeMap.put(ticker, volume);

                Double[] obv = utils.getOrDefaultIndicator(obvMap, ticker, 2);
                double currentOBV = calculate(close[0], close[1], volume[0], obv[1]);
                utils.updateIndicator(obv, currentOBV);
                obvMap.put(ticker, obv);

                Map<String, Object> result = new HashMap<>();
                result.put("time", time);
                result.put("ticker", ticker);
                result.put("obv", currentOBV);

                return objectMapper.writeValueAsString(result);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        });
        obvStream.to("OBV", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "OBVCalculator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        OBVCalculator obvCalculator = new OBVCalculator();
        KafkaStreams streams = new KafkaStreams(obvCalculator.createTopology(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
