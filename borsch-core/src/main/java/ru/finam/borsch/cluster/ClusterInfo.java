package ru.finam.borsch.cluster;


/**
 * Information about cluster for other users
 * Created by akhaymovich on 16.10.17.
 */
public interface ClusterInfo {

   int numOfMembers();
   int grpcPort();
   boolean isMyData(String shardKey);

   default int quorum(){
      int size = numOfMembers();
      if (size <= 2) {
         return size;
      } else {
         return size / 2 + 1;
      }
   }
}
