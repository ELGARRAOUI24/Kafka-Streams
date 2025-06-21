package ma.iibdcc;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamsApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("application.id", "Tp-Kafka-Streams");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceStream = builder.stream("weather-data");
        KStream<String, String> mapStream = sourceStream.flatMapValues(value -> Arrays.asList(value.split("\\|")));
        KStream<String, String> filterStream = mapStream.filter((key, value) -> Double.parseDouble(value.split(",")[1]) > 30);
        KStream<String, String> convertStrem = filterStream.mapValues(value -> value.split(",")[0]+","+String.valueOf((Double.parseDouble(value.split(",")[1])*9/5)+32)+","+value.split(",")[2]);
        KStream<String, String> keyStream = convertStrem.selectKey((key, value) -> value.split(",")[0]);
        KGroupedStream<String, String> groupStream = keyStream.groupByKey();
        KTable<String, String> calculStream = groupStream.aggregate(
                () -> "0.0,0.0,0",
                (key, value, agg) -> {
                    try {
                        String[] parts = value.split(",");
                        double temp = Double.parseDouble(parts[1]);
                        double hum = Double.parseDouble(parts[2]);
                        String[] aggParts = agg.split(",");
                        double sumTemp = Double.parseDouble(aggParts[0]) + temp;
                        double sumHum = Double.parseDouble(aggParts[1]) + hum;
                        long count = Long.parseLong(aggParts[2]) + 1;
                        return sumTemp + "," + sumHum + "," + count;
                    } catch (Exception e) {
                        return agg;
                    }
                },
                Materialized.with(Serdes.String(), Serdes.String())
        );

        KTable<String, String> averages = calculStream.mapValues((key, value) -> {
            String[] parts = value.split(",");
            double sumTemp = Double.parseDouble(parts[0]);
            double sumHum = Double.parseDouble(parts[1]);
            long count = Long.parseLong(parts[2]);
            double avgTemp = sumTemp / count;
            double avgHum = sumHum / count;
            return key + " : Température Moyenne = " + String.format("%.2f", avgTemp) + " °F, " + "Humidité Moyenne = " + String.format("%.2f", avgHum) + "%";
        });

        averages.toStream().to("station-averages", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}