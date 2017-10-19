package ru.finam.borsch.partitioner;


import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.finam.borsch.InetAddress;
import ru.finam.borsch.rpc.server.BorschServiceApi;
import java.util.List;


/**
 * Server partitioner. Use in one thread
 * Created by akhaymovich on 14.09.17.
 */
public class ServerHolder {

    private static final Logger LOG = LoggerFactory.getLogger(BorschServiceApi.class);


    private List<InetAddress> addressList;
    private final InetAddress ownAddress;
    private KetamaHashingRing hashingRing;

    public ServerHolder(InetAddress ownAddress,
                        List<InetAddress> addressList) {
        this.addressList = addressList;
        addressList.add(ownAddress);
        this.ownAddress = ownAddress;
        this.hashingRing = new KetamaHashingRing(addressList);
        LOG.info("Hash ring initialized. Num of servers : {}", addressList.size());
    }


    void addNewServer(InetAddress inetAddress) {
        LOG.info("Add new server {}", inetAddress);
        addressList.add(inetAddress);
        hashingRing = new KetamaHashingRing(addressList);
    }

    void removeServer(InetAddress inetAddress) {
        LOG.info("Server leave {}", inetAddress);
        addressList.remove(inetAddress);
        hashingRing = new KetamaHashingRing(addressList);
    }

    public boolean isMyData(ByteString accountHash) {
        InetAddress serverHolder = hashingRing.getServer(accountHash.toString());
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
