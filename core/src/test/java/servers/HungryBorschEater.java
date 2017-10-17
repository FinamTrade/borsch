package servers;

import ru.finam.borsch.InetAddress;

/**
 * Created by akhaymovich on 16.10.17.
 */
public class HungryBorschEater extends AbstractBorschEater {


    public HungryBorschEater(InetAddress grpcBorschAddress, String consulHost, int consulPort, int shard) {
        super(grpcBorschAddress, consulHost, consulPort, shard);
    }

    @Override
    protected String getServiceId() {
        return "HungryBorschEater";
    }

    private static final String consulHost = "consul-agent";
    private static final int consulPort = 8500;


    public static void main(String[] args) {
        InetAddress grpcAddress = new InetAddress("localhost", 50103);


        HungryBorschEater hungryBorschEater =
                new HungryBorschEater(grpcAddress, consulHost, consulPort, 1);

        hungryBorschEater.registerService();


        hungryBorschEater.launchBorsch();
        while (true){

        }
    }
}
