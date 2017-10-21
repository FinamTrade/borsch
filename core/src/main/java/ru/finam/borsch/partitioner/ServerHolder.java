package ru.finam.borsch.partitioner;


import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.HostPortAddress;
import ru.finam.borsch.rpc.server.BorschServiceApi;
import java.util.List;


/**
 * Server partitioner. Use in one thread
 * Created by akhaymovich on 14.09.17.
 */
public class ServerHolder {

    private static final Logger LOG = LoggerFactory.getLogger(BorschServiceApi.class);


    private List<HostPortAddress> addressList;
    private final HostPortAddress ownAddress;
    private KetamaHashingRing hashingRing;

    public ServerHolder(HostPortAddress ownAddress,
                        List<HostPortAddress> addressList) {
        this.addressList = addressList;
        addressList.add(ownAddress);
        this.ownAddress = ownAddress;
        this.hashingRing = new KetamaHashingRing(addressList);
        LOG.info("Hash ring initialized. Num of servers : {}", addressList.size());
    }


    void addNewServer(HostPortAddress hostPortAddress) {
        LOG.info("Add new server {}", hostPortAddress);
        addressList.add(hostPortAddress);
        hashingRing = new KetamaHashingRing(addressList);
    }

    void removeServer(HostPortAddress hostPortAddress) {
        LOG.info("Server leave {}", hostPortAddress);
        addressList.remove(hostPortAddress);
        hashingRing = new KetamaHashingRing(addressList);
    }

    public boolean isMyData(ByteString accountHash) {
        HostPortAddress serverHolder = hashingRing.getServer(accountHash.toString());
        return serverHolder.equals(ownAddress);
    }

    public int currentQuorum() {
        int size = addressList.size();
        if (size <= 2) {
            return size;
        } else {
            return addressList.size() / 2 + 1;
        }
    }

    public int numOfMembers() {
        return addressList.size();
    }

}
