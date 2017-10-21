package servers;

import ru.finam.borsch.HostPortAddress;

/**
 * Created by akhaymovich on 08.10.17.
 */
public class FatBorschEater extends AbstractBorschEater {


    public FatBorschEater(HostPortAddress grpcBorschAddress,
                         int shard) {
        super(grpcBorschAddress,  shard);
    }

    @Override
    protected String getServiceId() {
        return "FatBorschEater";
    }
}
