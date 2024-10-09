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

public class MACDCalculator extends IndicatorCalculator {
    // Moving Average Convergence Divergence
    private EMACalculator fastEMACalculator;
    private EMACalculator slowEMACalculator;
    private EMACalculator signalEMACalculator;
    private int fastPeriod;
    private int slowPeriod;
    private int signalPeriod;
    private String seriesType;

    public MACDCalculator(String seriesType, int fastPeriod, int slowPeriod, int signalPeriod) {
        this.fastPeriod = fastPeriod;
        this.slowPeriod = slowPeriod;
        this.signalPeriod = signalPeriod;
        this.seriesType = seriesType;
        this.fastEMACalculator = new EMACalculator(seriesType, fastPeriod);
        this.slowEMACalculator = new EMACalculator(seriesType, slowPeriod);
        this.signalEMACalculator = new EMACalculator(seriesType, signalPeriod);
    }

    public MACDCalculator() {
        this.fastPeriod = 12;
        this.slowPeriod = 26;
        this.signalPeriod = 9;
        this.seriesType = SeriesType.CLOSE;
        this.fastEMACalculator = new EMACalculator(seriesType, fastPeriod);
        this.slowEMACalculator = new EMACalculator(seriesType, slowPeriod);
        this.signalEMACalculator = new EMACalculator(seriesType, signalPeriod);
    }

    public void setSlowPeriod(int slowPeriod) {
        this.slowPeriod = slowPeriod;
    }

    public int getSlowPeriod() {
        return this.slowPeriod;
    }

    public void setFastPeriod(int fastPeriod) {
        this.fastPeriod = fastPeriod;
    }

    public int getFastPeriod() {
        return this.fastPeriod;
    }

    public void setSignalPeriod(int signalPeriod) {
        this.signalPeriod = signalPeriod;
    }

    public int getSignalPeriod() {
        return this.signalPeriod;
    }

    public void setSeriesType(String seriesType) {
        this.seriesType = seriesType;
    }

    public String getSeriesType() {
        return this.seriesType;
    }

    public double macdCalculate(double fastEMA, double slowEMA) {
        return fastEMA - slowEMA;
    }

    public double macdsCalculate(double macd, double previousSignalEMA) {
        return this.signalEMACalculator.calculate(macd, previousSignalEMA);
    }

    public double macdhCalculate(double macd, double macds) {
        return macd - macds;
    }

    @Override
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stockPrices = builder.stream("StockPrices");
        ObjectMapper objectMapper = new ObjectMapper();
        Utils utils = new Utils();

        Map<String, Integer[]> seriesMap = new HashMap<>();
        Map<String, Double[]> slowEMAMap = new HashMap<>();
        Map<String, Double[]> fastEMAMap = new HashMap<>();
        Map<String, Double[]> signalEMAMap = new HashMap<>();

        KStream<String, String> macdStream = stockPrices.mapValues((ValueMapper<String, String>) value -> {
            try {
                JsonNode jsonNode = objectMapper.readTree(value);

                String time = jsonNode.get("time").asText();
                String ticker = jsonNode.get("ticker").asText();

                Integer[] series = utils.getOrDefaultSeries(seriesMap, ticker, 1);
                utils.updateSeries(series, jsonNode.get(this.seriesType).asInt());
                seriesMap.put(ticker, series);

                Double[] fastEMA = utils.getOrDefaultIndicator(fastEMAMap, ticker, 2);
                double currentFastEMA = this.fastEMACalculator.calculate(series[0], fastEMA[1]);
                utils.updateIndicator(fastEMA, currentFastEMA);
                fastEMAMap.put(ticker, fastEMA);

                Double[] slowEMA = utils.getOrDefaultIndicator(slowEMAMap, ticker, 2);
                double currentSlowEMA = this.slowEMACalculator.calculate(series[0], slowEMA[1]);
                utils.updateIndicator(slowEMA, currentSlowEMA);
                slowEMAMap.put(ticker, slowEMA);

                double currentMACD = macdCalculate(currentFastEMA, currentSlowEMA);

                Double[] signalEMA = utils.getOrDefaultIndicator(signalEMAMap, ticker, 2);
                double currentSignalEMA = this.signalEMACalculator.calculate(currentMACD, signalEMA[1]);
                utils.updateIndicator(signalEMA, currentSignalEMA);
                signalEMAMap.put(ticker, signalEMA);

                double currentMACDS = macdsCalculate(currentMACD, signalEMA[1]);

                double currentMACDH = macdhCalculate(currentMACD, currentMACDS);

                Map<String, Object> result = new HashMap<>();
                result.put("time", time);
                result.put("ticker", ticker);
                result.put("macd", currentMACD);
                result.put("macds", currentMACDS);
                result.put("macdh", currentMACDH);

                return objectMapper.writeValueAsString(result);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        });
        macdStream.to("MACD", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "MACDCalculator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        MACDCalculator macdCalculator = new MACDCalculator();
        KafkaStreams streams = new KafkaStreams(macdCalculator.createTopology(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
