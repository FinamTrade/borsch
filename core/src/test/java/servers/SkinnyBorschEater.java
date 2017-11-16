package servers;

import ru.finam.borsch.HostPortAddress;

/**
 * Created by akhaymovich on 08.10.17.
 */
public class SkinnyBorschEater extends AbstractBorschEater {


    public SkinnyBorschEater(HostPortAddress grpcBorschAddress,
                             int shard) {
        super(grpcBorschAddress, shard);
    }

    @Override
    protected String getServiceId() {
        return "SkinnyBorschEater";
    }
}
