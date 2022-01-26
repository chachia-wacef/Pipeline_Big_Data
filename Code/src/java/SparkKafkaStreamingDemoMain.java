package org.thingsboard.samples.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
// Streaming
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
//from devices to TB 
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import scala.Tuple2;
import java.util.*;
//PostgreSQL DB
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;


public class SparkKafkaStreamingDemoMain {
    // Kafka brokers URL for Spark Streaming to connect and fetched messages from.
    private static final String KAFKA_BROKER_LIST = "localhost:9092";
    // Time interval in milliseconds of Spark Streaming Job, 10 seconds by default.
    private static final int STREAM_WINDOW_MILLISECONDS = 10000; // 10 seconds
    // Kafka telemetry topic to subscribe to. This should match to the topic in the rule action.
    private static final Collection<String> TOPICS = Arrays.asList("topic_sensor1","topic_sensor2"); //Topics
    // The application name
    public static final String APP_NAME = "Kafka Spark Streaming App";
    // Misc Kafka client properties
    private static Map<String, Object> getKafkaParams() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", KAFKA_BROKER_LIST);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "DEFAULT_GROUP_ID");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        return kafkaParams;
    }

    public static void main(String[] args) throws Exception {
        new StreamRunner().start();
    }

    @Slf4j
    private static class StreamRunner {
        private RestClient restClient;
        StreamRunner() throws Exception {
            restClient = new RestClient();
        }

        void start() throws Exception {
            //create tables in Postgresql if they don't exist
            create_db('Sensor1_tab')
            create_db('Sensor2_tab')
            //
            SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local");
            try (JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(STREAM_WINDOW_MILLISECONDS))) {  
                //2 consumers
                JavaInputDStream<ConsumerRecord<String, String>> stream1 =
                        KafkaUtils.createDirectStream(
                                ssc,
                                LocationStrategies.PreferConsistent(),
                                ConsumerStrategies.<String, String>Subscribe(TOPICS[0], getKafkaParams()) //Consumer1
                        );
                JavaInputDStream<ConsumerRecord<String, String>> stream2 =
                        KafkaUtils.createDirectStream(
                                ssc,
                                LocationStrategies.PreferConsistent(),
                                ConsumerStrategies.<String, String>Subscribe(TOPICS[1], getKafkaParams()) //Consumer2
                        );

                //Read data from TB and send it to database
                stream1.foreachRDD(rdd1 ->
                {   
                    //map received data to Sensor1DataMapper
                    JavaRDD<Sensor1Data> windRdd1 = rdd1.map(new Sensor1DataMapper());
                    //send data to Database
                    add_data_to_db('Sensor1_tab',windRdd1.id, windRdd1.value)
                    //Send data with spark streaming to use it with IA algorithm.
                    //
                });
                stream2.foreachRDD(rdd2 ->
                {   
                    //map received data to Sensor1DataMapper
                    JavaRDD<Sensor2Data> windRdd2 = rdd2.map(new Sensor2DataMapper());
                    //send data to Database
                    add_data_to_db('Sensor2_tab',windRdd2.id, windRdd2.value )
                    //Send data with spark streaming to use it with IA algorithm.
                    //
                });

                ssc.start();
                ssc.awaitTermination();
            }
        }

        //Fonction to Create a table in Postgresql
        private static void create_db(String table_name)
        {
            Connection c = null;
            Statement stmt = null;
            try {
                Class.forName("org.postgresql.Driver");
                c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/vehicledb","wacef", "123456"); 
                System.out.println("Opened database successfully");

                stmt = c.createStatement();
                String sql = "CREATE TABLE IF NOT EXISTS " + table_name + 
                    "(ID INT PRIMARY KEY     NOT NULL," +
                    " Value  DOUBLE NOT NULL";
                stmt.executeUpdate(sql);
                stmt.close();
                c.close();
            } catch ( Exception e ) {
                System.err.println( e.getClass().getName()+": "+ e.getMessage() );
                System.exit(0);
            }
             System.out.println("Table created successfully");
        }

        //Fonction to add data to a table in Postgresql
        private static void add_data_to_db(String table_name,String id,Double val)
        {
            Connection c = null;
            Statement stmt = null;
            try {
                Class.forName("org.postgresql.Driver");
                c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/vehicledb","wacef", "123456"); 
                c.setAutoCommit(false);
                System.out.println("Opened database successfully");

                stmt = c.createStatement();
                String sql = "INSERT INTO "+ table_name +  "(id,value) "
                    + "VALUES (" + id + " ," val.toString() + ");";
                stmt.executeUpdate(sql);
                stmt.close();
                c.commit();
                c.close();
            } catch (Exception e) {
                System.err.println( e.getClass().getName()+": "+ e.getMessage() );
                System.exit(0);
            }
            System.out.println("Records created successfully");
        }
    }



        private IMqttActionListener getCallback() {
            return new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    log.info("Telemetry data updated!");
                }
                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    log.error("Telemetry data update failed!", exception);
                }
            };
        }

        private static class Sensor1DataMapper implements Function<ConsumerRecord<String, String>, Sensor1Data> {
            private static final ObjectMapper mapper = new ObjectMapper();
            @Override
            public Sensor1Data call(ConsumerRecord<String, String> record) throws Exception {
                return mapper.readValue(record.value(), Sensor1Data.class);
            }
        }

        private static class Sensor2DataMapper implements Function<ConsumerRecord<String, String>, Sensor2Data> {
            private static final ObjectMapper mapper = new ObjectMapper();
            @Override
            public Sensor2Data call(ConsumerRecord<String, String> record) throws Exception {
                return mapper.readValue(record.value(), Sensor2Data.class);
            }
        }
}