

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * Created by user on 1/31/17.
 */
public class ItemSerializer implements  Closeable, AutoCloseable, Serializer<Item>, Deserializer<Item> {
    //private static final Logger logger = Logger.getLogger(ItemSerializer.class);

    private ObjectMapper mapper;

    public ItemSerializer() {
        this(null);
    }

    public ItemSerializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public static ItemSerializer defaultConfig() {
        return new ItemSerializer(new ObjectMapper());
    }

    public static ItemSerializer smileConfig() {
        return new ItemSerializer(new ObjectMapper(new SmileFactory()));
    }

    public Item deserialize(String s, byte[] bytes) {
        try {
            return mapper.readValue(bytes,SerializationHelper.class).to();
        }
        catch(IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    // was missing
    public void configure(Map<String, ?> map, boolean b) {
        if(mapper == null) {
            if("true".equals(map.get("value.serializer.jackson.smile"))) {
                mapper = new ObjectMapper(new SmileFactory());
            }
            else {
                mapper = new ObjectMapper();
            }
        }
    }

    public byte[] serialize(String s, Item item) {

        try {
            return mapper.writeValueAsBytes(SerializationHelper.from(item));
        }
        catch (JsonProcessingException e){
            throw new IllegalArgumentException(e);
        }
    }

    public void close() {
        mapper = null;
    }

    // helps serlialize
    public static class SerializationHelper {


        public String lastSeller;
        public String lastStashID;
        public String x;
        public String y;
        public int ilvl;
        public String icon;
        public String league;
        public String itemID;
        public String name;
        public String typeLine;
        public String note;
        public boolean identified;
        public boolean verified;
        public double price;
        //public RawFormat.PropertyList propertyList;
        public String explicitMods;
        public String flavorText;


        public static SerializationHelper from(Item packed) {
            SerializationHelper helper = new SerializationHelper();
            helper.lastSeller = packed.getLastSeller();
            helper.lastStashID = packed.getLastStashID();
            helper.x = packed.getX();
            helper.y = packed.getY();
            helper.ilvl = packed.getIlvl();
            helper.icon = packed.getIcon();
            helper.league = packed.getLeague();
            helper.itemID = packed.getItemID();
            helper.name = packed.getName();
            helper.typeLine = packed.getTypeLine();
            helper.note = packed.getNote();
            helper.identified = packed.getIdentified();
            helper.verified = packed.getVerified();
            helper.price = packed.getPrice();
            helper.explicitMods = packed.getExplicitMods();
            helper.flavorText = packed.getFlavourText();

            return helper;
        }

        public Item to() {
            Item unPacked = new Item();
            unPacked.setLastSeller(lastSeller);
            unPacked.setLastStashID(lastStashID);
            unPacked.setX(x);
            unPacked.setY(y);
            unPacked.setIlvl(ilvl);
            unPacked.setIcon(icon);
            unPacked.setLeague(league);
            unPacked.setItemID(itemID);
            unPacked.setName(name);
            unPacked.setTypeLine(typeLine);
            unPacked.setNote(note);
            unPacked.setIdentified(identified);
            unPacked.setVerified(verified);
            //no set price
            unPacked.setExplicitMods(explicitMods);
            unPacked.setFlavourText(flavorText);
            return unPacked;
        }
    }
}
