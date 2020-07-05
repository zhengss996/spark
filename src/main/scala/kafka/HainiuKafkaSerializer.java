package kafka;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

public class HainiuKafkaSerializer implements Serializer {
    public void configure(Map configs, boolean isKey) {

    }

    public byte[] serialize(String topic, Object data) {
        if(data == null){
            return null;
        }else{
            byte[] bytes = null;
            ByteArrayOutputStream bo = null;
            ObjectOutputStream oo = null;
            try{
                bo = new ByteArrayOutputStream();
                oo = new ObjectOutputStream(bo);
                oo.writeObject(data);
                bytes = bo.toByteArray();
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                try{
                    oo.close();
                    bo.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            return bytes;
        }
    }

    public void close() {

    }
}