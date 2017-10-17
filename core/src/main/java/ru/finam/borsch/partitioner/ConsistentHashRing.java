package ru.finam.borsch.partitioner;

import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.InetAddress;
import ru.finam.borsch.rpc.server.BorschServiceApi;

import java.util.ArrayList;
import java.util.List;


/**
 * Server partitioner. Use in one thread
 * Created by akhaymovich on 14.09.17.
 */
public class ConsistentHashRing {

    private static final Logger LOG = LoggerFactory.getLogger(BorschServiceApi.class);
    private List<InetAddress> addressList;
    private final InetAddress ownAddress;

    public ConsistentHashRing(InetAddress ownAddress,
                              List<InetAddress> addressList) {
        addressList.add(ownAddress);
        this.ownAddress = ownAddress;
        this.addressList = configureAddressList(addressList);
        LOG.info("Hash ring initialized. Num of servers : {}", addressList.size());
    }


    void addNewServer(InetAddress inetAddress) {
        LOG.info("Add new server {}", inetAddress);
        addressList.add(inetAddress);
        addressList = configureAddressList(addressList);
    }

    void removeServer(InetAddress inetAddress) {
        LOG.info("Server leave {}", inetAddress);
        addressList.remove(inetAddress);
        addressList = configureAddressList(addressList);
    }

    public boolean isMyData(ByteString accountHash) {
        int numOfShard = Hashing.consistentHash(accountHash.hashCode(), addressList.size());
        InetAddress inetAddress = addressList.get(numOfShard);
        return inetAddress.equals(ownAddress);
    }

    public int currentQuorum() {
        int size = addressList.size();
        if (size <= 2){
            return size;
        } else {
            return addressList.size() / 2 + 1;
        }
    }

    public int numOfMembers() {
        return addressList.size();
    }

    private static List<InetAddress> configureAddressList(List<InetAddress> dictionary) {
        int size = dictionary.size();
        List<InetAddress> addressList = new ArrayList<>(dictionary);
        for (InetAddress inetAddress : dictionary) {
            int index = Math.abs(inetAddress.hashCode()) % size;
            addressList.set(index, inetAddress);
        }
        return addressList;
    }
}
