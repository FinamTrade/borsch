package ru.finam.rocksdb;

import com.google.protobuf.Timestamp;
import finam.protobuf.borsch.KV;
import finam.protobuf.borsch.KVRecord;

import java.util.List;
import java.util.Optional;


/**
 * Created by akhaymovich on 05.09.17.
 */
public interface Store extends UpdateTimeGetter{


    boolean put(KV kv);

    Optional<KV> get(String familyName, byte[] key);

    List<KVRecord> getDbCopy(Timestamp millisFrom);

    List<KVRecord> getColumnCopy(String columnName);

    double getRecordSize();

    void loadSnapshot(List<KVRecord> kvRecordList);


}
