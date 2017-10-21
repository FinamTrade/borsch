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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Implementation of Store
 * Created by akhaymovich on 01.09.17.
 */
public class RocksDbStore implements Store {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDbStore.class);

    private final RocksDB db;
    private final Map<String, ColumnFamilyHandle> handles = new ConcurrentHashMap<>();

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

        } catch (RocksDBException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException("Coudn't load db from " + location);
        }


        try {
            db = RocksDB.open(dbOptions, location,
                    familyList,
                    columns);
        } catch (RocksDBException e) {
            throw new RuntimeException("Invalid rocks db state", e);
        }

        for (int i = 0; i < familyList.size(); i++) {
            String columnFamilyName = new String(familyList.get(i).columnFamilyName());
            handles.put(columnFamilyName, columns.get(i));
            i++;
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
            return false;
        }
        KVRecord kvRecord = createRecord(kv);
        try {
            db.put(columnFamilyHandle, new WriteOptions(),
                    kv.getKey().toByteArray(),
                    kvRecord.toByteArray());
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
    public List<KVRecord> getDbCopy(long millisFrom) {
        List<KVRecord> allRecords = new ArrayList<>();
        Snapshot snapshot = db.getSnapshot();
        handles.values().forEach(columnFamilyHandle -> {
            List<KVRecord> records = readFamilyData(columnFamilyHandle, snapshot);
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
        return readFamilyData(columnFamilyHandle, snapshot);
    }

    private List<KVRecord> readFamilyData(ColumnFamilyHandle columnFamilyHandle,
                                          Snapshot snapshot) {
        ReadOptions readOptions = new ReadOptions();
        readOptions.setSnapshot(snapshot);
        RocksIterator rocksIterator = db.newIterator(columnFamilyHandle, readOptions);
        List<KVRecord> result = new ArrayList<>();
        for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
            byte[] value = rocksIterator.value();
            try {
                KVRecord kvRecord = KVRecord.parseFrom(value);
                result.add(kvRecord);
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
        return result;
    }


    @Override
    public void loadSnapshot(List<KVRecord> kvRecordList) {
        LOG.info("Get {} records from partner ", kvRecordList.size());
        if (kvRecordList.isEmpty()) {
            return;
        }
        for (KVRecord kvRecord : kvRecordList) {
            KV kv = kvRecord.getKv();
            KVRecord prev = getRecord(kv.getColumnFamily(), kv.getKey().toByteArray());
            if (prev == null || prev.getUpdateTime().getSeconds()
                    <= kvRecord.getUpdateTime().getSeconds()) {
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
                //todo
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

    private static KVRecord createRecord(KV kv) {
        long currentSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(currentSeconds)
                .build();
        return KVRecord.newBuilder().setKv(kv).setUpdateTime(timestamp).build();
    }
}
