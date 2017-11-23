package ru.finam.borsch.cluster;

import com.google.protobuf.ByteString;

/**
 * Information about cluster for other users
 * Created by akhaymovich on 16.10.17.
 */
public interface ClusterInfo {

   int numOfMembers();
   int grpcPort();
   boolean isMyData(ByteString shardKey);

   default int quorum(){
      int size = numOfMembers();
      if (size <= 2) {
         return size;
      } else {
         return size / 2 + 1;
      }
   }
}
