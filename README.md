The implementation of Distributed RocksDb for several servers.

The main purpose of this project is to provide an opportunity to use RocksDb
in case when there are several servers in the system and all their databases must be
synchronized.

Borsch node registers in consul after start and than looks for a neighbours in the cluster.
All neighbours are added to consistent hash ring of 1024 size.
In Db data stores as a key-value. Key has a complex structure which consists of two
parts: entity_id and shard_id. Shard_id is important because it determines which server
will be processing this entity.


Usage

use {gradle uploadArchives} to build jar file