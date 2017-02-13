import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.exc.ReqlOpFailedError;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;


// read data from kafka topic
// insert into rethink db
public class InsertLookUp {



    private static final RethinkDB r = RethinkDB.r;
    private static KafkaConsumer<String, String> consumer;
    private static final String TABLE_NAME = "lookUp";


    public static void main(String[] args){

        String dbName = args[0];
        Properties props = new Properties();

        String topic = "poe4";
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "zookeeper"); // need to test if zookeeper is required group, it works but do other groups work?
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("fetch.message.max.bytes","100000");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));

        Connection conn = r.connection().hostname(dbName).port(28015).connect();
        conn.use("poeapi");
        try {

            r.db("poeapi").tableCreate(TABLE_NAME).run(conn);
        }
        catch (ReqlOpFailedError oops){
            // don't die if table no exist
            System.out.println("table already exists");

            consumeLoop(conn);

        }
        conn.close();
    }
    // loop to consume poe4 topic and insert to rethinkdb
    private static void consumeLoop(Connection conn){


        while(true){
            ConsumerRecords<String, String> records = consumer.poll(1000);
            ObjectMapper om = new ObjectMapper();
            MapObject bucket = r.hashMap();
            for (ConsumerRecord<String, String> record : records) {
                JsonNode jn = null;
                try {
                    jn = om.readTree(record.value());

                    try{
                        String id = jn.get("cleanName").asText();
                        String avgPrice = jn.get("avgPrice").asText();
                        String STD = jn.get("STD").asText();
                        String threshold = jn.get("threshold").asText();
                        bucket.with("id", id)
                                .with("name", id)
                                .with("avgPrice",Double.parseDouble(avgPrice))
                                .with("STD",Double.parseDouble(STD) )
                                .with("threshold", Double.parseDouble(threshold));
                    }catch(NullPointerException npe){
                        npe.printStackTrace();
                    }
                }catch(Exception e){
                    System.out.println("fooooooo");
                    e.printStackTrace();
                }

            }
            r.table(TABLE_NAME).insert(bucket).optArg("conflict","replace").run(conn);
        }

    }

}
