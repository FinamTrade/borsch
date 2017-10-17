package servers;

import ru.finam.borsch.InetAddress;

/**
 * Created by akhaymovich on 08.10.17.
 */
public class SkinnyBorschEater extends AbstractBorschEater {


    public SkinnyBorschEater(InetAddress grpcBorschAddress,
                             String consulHost,
                             int consulPort,
                             int shard) {
        super(grpcBorschAddress, consulHost, consulPort, shard);
    }

    @Override
    protected String getServiceId() {
        return "SkinnyBorschEater";
    }
}
