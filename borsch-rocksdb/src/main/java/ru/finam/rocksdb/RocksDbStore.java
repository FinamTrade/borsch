package ru.finam.rocksdb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import finam.protobuf.borsch.KV;
import finam.protobuf.borsch.KVRecord;
import org.rocksdb.*;
import org.slf4j.*;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


/**
 * Implementation of Store
 * Created by akhaymovich on 01.09.17.
 */
public class RocksDbStore implements Store, UpdateTimeGetter {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDbStore.class);

    private final RocksDB db;
    private final Map<String, ColumnFamilyHandle> handles = new ConcurrentHashMap<>();
    private final AtomicLong lastTime = new AtomicLong(0);


    static {
        RocksDB.loadLibrary();
    }


    public RocksDbStore(String location) {
        List<ColumnFamilyHandle> columns = new ArrayList<>();
        DBOptions dbOptions = createDbOptions();
        List<ColumnFamilyDescriptor> familyList;
        try {
            List<byte[]> families = RocksDB.listColumnFamilies(new Options(), location);
            familyList = families.stream()
                    .map(familyName -> new ColumnFamilyDescriptor(familyName))
                    .collect(Collectors.toList());
            familyList.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
            families.stream().forEach(bytes -> System.out.println((new String(bytes))));

        } catch (RocksDBException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException("Coudn't load db from " + location);
        }


        try {
            db = RocksDB.open(dbOptions, location,
                    familyList,
                    columns);
            LOG.info("Load data from rocks db {}", location);

        } catch (RocksDBException e) {
            throw new RuntimeException("Invalid rocks db state", e);
        }

        for (int i = 0; i < familyList.size(); i++) {
            byte[] columnName = familyList.get(i).columnFamilyName();
            if (columnName.length == 0) {
                continue;
            }
            String columnFamilyName = new String(columnName);
            handles.put(columnFamilyName, columns.get(i));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            dbOptions.close();
            db.close();
        }));
    }


    @Override
    public boolean put(KV kv) {
        ColumnFamilyHandle columnFamilyHandle = getHandle(kv.getColumnFamily());
        if (columnFamilyHandle == null) {
            LOG.error("No column family name!");
            return false;
        }
        KVRecord kvRecord = createRecord(kv);
        try {
            db.put(columnFamilyHandle, new WriteOptions(),
                    kv.getKey().toByteArray(),
                    kvRecord.toByteArray());
            lastTime.getAndSet(System.currentTimeMillis());
            return true;
        } catch (RocksDBException e) {
            LOG.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public Optional<KV> get(String familyName, byte[] key) {
        ColumnFamilyHandle columnFamilyHandle = getHandle(familyName);
        try {
            byte[] val = db.get(columnFamilyHandle, key);
            if (val == null || val.length == 0) {
                return Optional.empty();
            }
            KVRecord kvRecord = KVRecord.parseFrom(ByteString.copyFrom(val));
            return Optional.of(kvRecord.getKv());
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
            return Optional.empty();
        }
    }

    @Override
    public List<KVRecord> getDbCopy(Timestamp dataFrom) {
        List<KVRecord> allRecords = new ArrayList<>();
        Snapshot snapshot = db.getSnapshot();
        handles.values().forEach(columnFamilyHandle -> {
            List<KVRecord> records = readFamilyData(columnFamilyHandle, snapshot, dataFrom);
            allRecords.addAll(records);
        });
        return allRecords;
    }


    @Override
    public List<KVRecord> getColumnCopy(String columnName) {
        if (columnName.isEmpty()) {
            return Collections.emptyList();
        }
        Snapshot snapshot = db.getSnapshot();
        ColumnFamilyHandle columnFamilyHandle = getHandle(columnName);
        return readFamilyData(columnFamilyHandle, snapshot, Timestamp.getDefaultInstance());
    }

    @Override
    public double getRecordSize() {
        return 0;
    }

    private List<KVRecord> readFamilyData(ColumnFamilyHandle columnFamilyHandle,
                                          Snapshot snapshot,
                                          Timestamp dataFrom) {
        ReadOptions readOptions = new ReadOptions();
        readOptions.setSnapshot(snapshot);
        RocksIterator rocksIterator = db.newIterator(columnFamilyHandle, readOptions);
        List<KVRecord> result = new ArrayList<>();
        for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
            byte[] value = rocksIterator.value();
            try {
                KVRecord kvRecord = KVRecord.parseFrom(value);
                if (kvRecord.getUpdateTime().getSeconds() > dataFrom.getSeconds()) {
                    result.add(kvRecord);
                }
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
        return result;
    }


    @Override
    public void loadSnapshot(List<KVRecord> kvRecordList) {
        if (kvRecordList.isEmpty()) {
            return;
        }
        for (KVRecord kvRecord : kvRecordList) {
            KV kv = kvRecord.getKv();
            KVRecord prev = getRecord(kv.getColumnFamily(), kv.getKey().toByteArray());
            if (prev == null || prev.getUpdateTime().getSeconds()
                    < kvRecord.getUpdateTime().getSeconds()) {
                put(kv);
            }
        }
    }


    @Nullable
    private KVRecord getRecord(String familyName, byte[] key) {
        ColumnFamilyHandle columnFamilyHandle = getHandle(familyName);
        try {
            byte[] val = db.get(columnFamilyHandle, key);
            if (val == null || val.length == 0) {
                return null;
            }
            return KVRecord.parseFrom(val);
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
    }

    private ColumnFamilyHandle getHandle(String family) {
        String name = family;
        return handles.computeIfAbsent(name, familyKey -> {
            ColumnFamilyHandle handle = null;
            try {
                byte[] bytes = familyKey.getBytes();
                ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(bytes);
                handle = db.createColumnFamily(descriptor);
            } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
                throw new RuntimeException("Can't create family {}" + family);
            }
            return handle;
        });
    }

    private static DBOptions createDbOptions() {
        return new DBOptions()
                .setCreateIfMissing(true)
                .setLogFileTimeToRoll(60)
                .setKeepLogFileNum(3)
                .setRecycleLogFileNum(3)
                .setMaxTotalWalSize(50 * 1024 * 1024)
                .setCreateMissingColumnFamilies(true);
    }

    //Compute Timestamp from Java `System.currentTimeMillis()`.
    //https://developers.google.com/protocol-buffers/docs/reference/java/com/google/protobuf/Timestamp
    private static KVRecord createRecord(KV kv) {
        long millis = System.currentTimeMillis();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(millis / 1000)
                .setNanos((int) ((millis % 1000) * 1000000)).build();
        return KVRecord.newBuilder().setKv(kv).setUpdateTime(timestamp).build();
    }

    @Override
    public long getLastUpdateTime() {
        return lastTime.get();
    }
}
