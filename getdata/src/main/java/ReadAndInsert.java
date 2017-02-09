import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.exc.ReqlOpFailedError;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;


// read data from kafka topic
// insert into rethink db
public class ReadAndInsert {



    public static final RethinkDB r = RethinkDB.r;
    private static KafkaConsumer<String, String> consumer;
    private static final String TABLE_NAME = "itemList";


    public static void main(String[] args){

        String dbName = args[0];
        Properties props = new Properties();
        // THIS WORKS NOW
        String topic = "poe3";
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
            //r.db("poeapi").tableDrop("itemList").run(conn);

            r.db("poeapi").tableCreate(TABLE_NAME).run(conn);

        }
        catch (ReqlOpFailedError oops){
            // don't die if table no exist
            System.out.println("table already exists");

            consumeLoop(conn);

//            Cursor cursor = r.table("tv_shows").run(conn);
//            for (Object doc : cursor) {
//                System.out.println(doc);
//            }
        }
        //consumeLoop(conn);
        conn.close();
    }

    // loop to consume poe3 topic and insert to rethinkdb
    private static void consumeLoop(Connection conn){


        while(true){
            ConsumerRecords<String, String> records = consumer.poll(1000);
            ObjectMapper om = new ObjectMapper();
            //JsonNode[] bucket = new JsonNode[records.count()];
            MapObject bucket = r.hashMap();
            for (ConsumerRecord<String, String> record : records) {

                JsonNode jn = null;
                try {
                    jn = om.readTree(record.value());

                    //System.out.println(jn.toString());


                    // make key pretty
                    String cleanName = jn.get("cleanName").asText();

                    Double value = jn.get("price").asDouble();

                    bucket.with("id",jn.get("id").asText())
                            .with("itemName",cleanName)
                            .with("count", jn.get("count").asText())
                            .with("sellerID", jn.get("accountName").asText())
                            //.with("itemID", jn.get("id").asText())
                            .with("x", jn.get("x").asText())
                            .with("y", jn.get("y").asText())
                            .with("note", jn.get("note").asText())
                            .with("icon", jn.get("icon").asText())
                            .with("league", jn.get("league").asText())
                            .with("price", value)
                            .with("stashName", jn.get("stashName").asText())
                            .with("privateMessage", jn.get("privateMessage").asText());
//                    String count = jn.get("count").asText();
//                    String accountName =jn.get("accountName").asText();
//                    String id =jn.get("id").asText();
//                    String x = jn.get("x").asText();
//                    String y = jn.get("y").asText();
//                    String note =  jn.get("note").asText();
//                    String icon = jn.get("icon").asText();
//                    String league = jn.get("league").asText();


//                    r.table("itemCount").insert(r.hashMap("id",key)
//                            .with("itemName",key)
//                            .with("count",value)
//                            .with("count", jn.get("count"))
//                            .with("sellerID", jn.get("accountName").asText())
//                            .with("itemID", jn.get("id").asText())
//                            .with("x", jn.get("x").asText())
//                            .with("y", jn.get("y").asText())
//                            .with("note", jn.get("note").asText())
//                            .with("icon", jn.get("icon").asText())
//                            .with("league", jn.get("league").asText())

 //                   ).optArg("conflict","replace").run(conn);
//                    if(bucket.size() > 200){
//                        r.table("itemCount").insert(bucket).optArg("conflict","replace").run(conn);
//                    }
                }catch(IOException ioe){
                    System.out.println("fooooooo");
                    ioe.printStackTrace();
                }

            }
            r.table(TABLE_NAME).insert(bucket).optArg("conflict","replace").run(conn);
        }
    }

}
