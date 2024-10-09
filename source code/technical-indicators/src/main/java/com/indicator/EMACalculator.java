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

public class EMACalculator extends IndicatorCalculator {
    // Exponential Moving Average
    private int period;
    private double alpha;
    private String seriesType;

    public EMACalculator(String seriesType, int period) {
        this.seriesType = seriesType;
        this.period = period;
        this.alpha = (double) 2 / (this.period + 1);
    }

    public EMACalculator() {
        this.seriesType = SeriesType.CLOSE;
        this.period = 10;
        this.alpha = (double) 2 / (this.period + 1);
    }

    public void setPeriod(int period) {
        this.period = period;
        this.alpha = (double) 2 / (this.period + 1);
    }

    public int getPeriod() {
        return this.period;
    }

    public double getAlpha() {
        return this.alpha;
    }

    public void setSeriesType(String seriesType) {
        this.seriesType = seriesType;
    }

    public String getSeriesType() {
        return this.seriesType;
    }

    public double calculate(int x, double previousEMA) {
        return previousEMA + this.alpha * (x - previousEMA);
    }

    public double calculate(double x, double previousEMA) {
        return previousEMA + this.alpha * (x - previousEMA);
    }

    @Override
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stockPrices = builder.stream("StockPrices");
        ObjectMapper objectMapper = new ObjectMapper();
        Utils utils = new Utils();

        Map<String, Integer[]> seriesMap = new HashMap<>();
        Map<String, Double[]> emaMap = new HashMap<>();

        KStream<String, String> emaStream = stockPrices.mapValues((ValueMapper<String, String>) value -> {
            try {
                JsonNode jsonNode = objectMapper.readTree(value);

                String time = jsonNode.get("time").asText();
                String ticker = jsonNode.get("ticker").asText();

                Integer[] series = utils.getOrDefaultSeries(seriesMap, ticker, 1);
                utils.updateSeries(series, jsonNode.get(this.seriesType).asInt());
                seriesMap.put(ticker, series);

                Double[] ema = utils.getOrDefaultIndicator(emaMap, ticker, 2);
                double currentEMA = calculate(series[0], ema[1]);
                utils.updateIndicator(ema, currentEMA);
                emaMap.put(ticker, ema);

                Map<String, Object> result = new HashMap<>();
                result.put("time", time);
                result.put("ticker", ticker);
                result.put("ema", currentEMA);

                return objectMapper.writeValueAsString(result);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        });
        emaStream.to("EMA", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "EMACalculator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        EMACalculator emaCalculator = new EMACalculator();
        KafkaStreams streams = new KafkaStreams(emaCalculator.createTopology(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
