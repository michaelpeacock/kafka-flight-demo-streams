package io.confluent.flightdemo;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
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

public class USFlightCounter {


    public static void main(String[] args) {
        System.out.println("In USFlightCounter...");
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "us-flight-counter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        config.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, GenericRecord> flights = builder.stream("flights_raw");
        KTable<String, Long> usFlightCounts = flights
                .filter((key, value) -> {
                    //System.out.println("key: " + key + " country: " + value.get("originCountry"));
                    return value.get("originCountry").toString().matches("United States");
                })
                .selectKey((key, value) -> "total-us-flights")
                .groupByKey()
                .count();

        final KTable<String, String> dashboardData = usFlightCounts
                .mapValues(new ValueMapper<Long, String>() {
                    @Override
                    public String apply(final Long count) {
                        DashboardModel dashboard = new DashboardModel();
                        dashboard.setDashboardTitle("Total US Flights");
                        dashboard.setDashboardValue(count.toString());
                        return dashboard.toJSON();
                }
        });
        dashboardData.toStream().to("dashboard-data", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

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
