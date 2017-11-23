package servers;

import com.google.protobuf.Timestamp;
import finam.protobuf.borsch.KV;
import finam.protobuf.borsch.KVRecord;
import ru.finam.rocksdb.RocksDbStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RocksDbComparator {

    private RocksDbStore store1;
    private RocksDbStore store2;

    private RocksDbComparator(String location1, String location2) {
        store1 = new RocksDbStore(location1);
        store2 = new RocksDbStore(location2);
    }

    private boolean compare() {
        HashMap<String, KV> first = compareRecords(store1);
        HashMap<String, KV> second = compareRecords(store2);
        System.out.println("Num of records in first db " + first.size());
        System.out.println("Num of records in second db " + second.size());

        for (Map.Entry<String, KV> kvEntry : first.entrySet()) {
            if (!second.containsKey(kvEntry.getKey())) {
                return false;
            }
        }

        for (Map.Entry<String, KV> kvEntry : second.entrySet()) {
            if (!first.containsKey(kvEntry.getKey())) {
                return false;
            }
        }
        return true;
    }

    private static HashMap<String, KV> compareRecords(RocksDbStore store) {
        List<KVRecord> recordList1 = store.getDbCopy(Timestamp.getDefaultInstance());
        HashMap<String, KV> store1Vals = new HashMap<>();
        for (KVRecord record : recordList1) {
            KV kv = record.getKv();
            store1Vals.put(new String(kv.getKey().toByteArray()), kv);
        }
        return store1Vals;
    }


    public static void main(String[] args) {
        String loc1 = "/var/lib/borsch/db/SkinnyBorschEater";
        String loc2 = "/var/lib/borsch/db/FatBorschEater";
        RocksDbComparator comparator = new RocksDbComparator(loc1, loc2);
        System.out.println("Result : " + comparator.compare());
    }

}
