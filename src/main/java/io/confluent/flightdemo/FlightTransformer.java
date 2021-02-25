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
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.models.FlightModel;

public class FlightTransformer {

    public static void main(String[] args) {
        System.out.println("In FlightTransformer...");
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "flight-model-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        config.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, GenericRecord> flights = builder.stream("flights_raw");
        KStream<String, String> flightModel = flights.mapValues(new ValueMapper<GenericRecord, String>() {
            @Override
            public String apply(final GenericRecord record) {
                return FlightTransformer.createFlightModel(record).toJSON();
            }
        });
        flightModel.to("flights", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Update:
        // print the topology every 10 seconds for learning purposes
        while (true) {
            // System.out.println(streams.toString());
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }

    }

    static public FlightModel createFlightModel(GenericRecord record) {
        System.out.println(record.toString());
        FlightModel flight = new FlightModel();

        if (record.get("id") != null) {
            flight.setId(record.get("id").toString());
        }

        if (record.get("callsign") != null) {
            flight.setCallSign(record.get("callsign").toString());
        }

        if (record.get("originCountry") != null) {
            flight.setOriginCountry(record.get("originCountry").toString());
        }

        if (record.get("timePosition") != null) {
            flight.setUpdateTime(Long.valueOf(record.get("timePosition").toString()));
        }

        if (record.get("location") != null) {
            flight.setLatitude(Double.valueOf(((GenericRecord) record.get("location")).get("lat").toString()));
            flight.setLongitude(Double.valueOf(((GenericRecord) record.get("location")).get("lon").toString()));
        }

        if (record.get("geometricAltitude") != null) {
            flight.setAltitude(Double.valueOf(record.get("geometricAltitude").toString()));
        }

        if (record.get("velocity") != null) {
            flight.setSpeed(Double.valueOf(record.get("velocity").toString()));
        }

        if (record.get("heading") != null) {
            flight.setHeading(Double.valueOf(record.get("heading").toString()));
        }

        return flight;
    }

}
