package io.confluent.flightdemo;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.models.DashboardModel;

public class FlightCounter {
    public static void main(String[] args) {
        System.out.println("In FlightCounter...");
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "flight-counter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        config.put("schema.registry.url", "http://localhost:8081");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Object> flights = builder.stream("flights_raw");
        KTable<String, Long> flightCounts = flights
                .selectKey((key, value) -> "total-flights-processed")
                .groupByKey()
                .count();

        final KTable<String, String> dashboardData = flightCounts
                .mapValues(new ValueMapper<Long, String>() {
                    @Override
                    public String apply(final Long count) {
                        DashboardModel dashboard = new DashboardModel();
                        dashboard.setDashboardTitle("Total Flights Processed");
                        dashboard.setDashboardValue(count.toString());
                        return dashboard.toJSON();
                }
        });
        dashboardData.toStream().to("dashboard-data", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        // print the topology
        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Update:
        // print the topology every 10 seconds for learning purposes
        while(true){
            //System.out.println(streams.toString());
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }


    }
}
