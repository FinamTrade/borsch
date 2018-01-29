package servers;

import com.google.protobuf.ByteString;
import finam.protobuf.borsch.KV;
import finam.protobuf.borsch.Key;
import finam.protobuf.borsch.PutRequest;
import finam.protobuf.borsch.WriteMode;
import java.nio.charset.Charset;

/**
 * Created by akhaymovich on 08.10.17.
 */
public class BorschDataThread {


    public BorschDataThread(SimpleGrpcClient dataProducer,
                            int shard,
                            String name) {

        Thread t = new Thread(() -> {
            int counter = Integer.MAX_VALUE;

            for (int i = 0; i < 10000000; i++) {

                ByteString bytes =
                        ByteString.copyFrom(String.valueOf(counter).getBytes());
                counter--;
                Key key = Key.newBuilder()
                        .setEntityPart(fromString(String.valueOf(counter)))
                        .setShardPart(fromString(String.valueOf(shard)))
                        .build();


                KV kv = KV.newBuilder()
                        .setColumnFamily("test")
                        .setValue(bytes)
                        .setKey(key)
                        .build();

                PutRequest putRequest = PutRequest.newBuilder()
                        .setMode(WriteMode.ALL)
                        .setKv(kv)
                        .build();
                dataProducer.put(putRequest);
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();
    }

    private static ByteString fromString(String value) {
        return ByteString.copyFrom(value, Charset.defaultCharset());
    }


}
