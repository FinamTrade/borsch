package servers;

import ru.finam.borsch.HostPortAddress;

/**
 * Created by akhaymovich on 16.10.17.
 */
public class HungryBorschEater extends AbstractBorschEater {


    public HungryBorschEater(HostPortAddress grpcBorschAddress,
                             int shard) {
        super(grpcBorschAddress, shard);
    }

    @Override
    protected String getServiceId() {
        return "HungryBorschEater";
    }


    public static void main(String[] args) {
        HostPortAddress grpcAddress = new HostPortAddress("localhost", 50103);


        HungryBorschEater hungryBorschEater =
                new HungryBorschEater(grpcAddress, 1);

        hungryBorschEater.registerService();


        hungryBorschEater.launchBorsch();
        while (true) {

        }
    }
}
