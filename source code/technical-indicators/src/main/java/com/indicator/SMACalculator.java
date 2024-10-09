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

public class SMACalculator extends IndicatorCalculator {
    // Simple Moving Average
    private int period;
    private String seriesType;

    public SMACalculator(String seriesType, int period) {
        this.period = period;
        this.seriesType = seriesType;
    }

    public SMACalculator() {
        this.period = 10;
        this.seriesType = SeriesType.CLOSE;
    }

    public void setPeriod(int period) {
        this.period = period;
    }

    public int getPeriod() {
        return this.period;
    }

    public void setSeriesType(String seriesType) {
        this.seriesType = seriesType;
    }

    public String getSeriesType() {
        return this.seriesType;
    }

    public double calculate(Integer[] series) {
        int sum = 0;
        for (int val : series) {
            sum += val;
        }
        return (double) sum / series.length;
    }

    @Override
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stockPrices = builder.stream("StockPrices");
        ObjectMapper objectMapper = new ObjectMapper();
        Utils utils = new Utils();

        Map<String, Integer[]> seriesMap = new HashMap<>();

        KStream<String, String> smaStream = stockPrices.mapValues((ValueMapper<String, String>) value -> {
            try {
                JsonNode jsonNode = objectMapper.readTree(value);

                String time = jsonNode.get("time").asText();
                String ticker = jsonNode.get("ticker").asText();

                Integer[] series = utils.getOrDefaultSeries(seriesMap, ticker, this.period);
                utils.updateSeries(series, jsonNode.get(this.seriesType).asInt());
                seriesMap.put(ticker, series);

                double currentSMA = this.calculate(series);

                Map<String, Object> result = new HashMap<>();
                result.put("time", time);
                result.put("ticker", ticker);
                result.put("sma", currentSMA);

                return objectMapper.writeValueAsString(result);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        });
        smaStream.to("SMA", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "SMA");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        SMACalculator smaCalculator = new SMACalculator();
        KafkaStreams streams = new KafkaStreams(smaCalculator.createTopology(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
