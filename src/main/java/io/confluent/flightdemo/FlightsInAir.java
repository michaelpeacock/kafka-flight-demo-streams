package io.confluent.flightdemo;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.util.HashMap;
import java.util.Map;
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

public class FlightsInAir {
    public static void main(String[] args) {
        System.out.println("In FlightsInAir...");
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "current-flights-in-air-counter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        config.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        Map<String, String> uniqueFlightsMap = new HashMap<String, String>();
        

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, GenericRecord> flights = builder.stream("flights_raw");
        KTable<String, Long> uniquesFlightCountsByKey = flights
                .filter((key, value) ->  {return value.get("onGround").toString().matches("false");})
                .selectKey((key, value) -> {
                    uniqueFlightsMap.put(key, value.get("id").toString());
                    return "flights-in-air-counts";
                })
                .groupByKey()
                .count();
        uniquesFlightCountsByKey.toStream().to("flights-in-air", Produced.with(Serdes.String(), Serdes.Long()));

       final KTable<String, String> dashboardData = uniquesFlightCountsByKey
                .mapValues(new ValueMapper<Long, String>() {
                    @Override
                    public String apply(final Long count) {
                        DashboardModel dashboard = new DashboardModel();
                        dashboard.setDashboardTitle("Flights In Air");
                        dashboard.setDashboardValue(String.valueOf(uniqueFlightsMap.size()));
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
