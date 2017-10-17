package servers;

import ru.finam.borsch.InetAddress;

/**
 * Created by akhaymovich on 08.10.17.
 */
public class LunchLauncher {

    private static final String consulHost = "consul-agent";
    private static final int consulPort = 8500;


    public static void main(String[] args) {
        InetAddress grpcAddress1 = new InetAddress("localhost", 50101);
        InetAddress grpcAddress2 = new InetAddress("localhost", 50102);

        FatBorschEater fatBorschEater =
                new FatBorschEater(grpcAddress1, consulHost, consulPort, 1);

        SkinnyBorschEater skinnyBorschEater =
                new SkinnyBorschEater(grpcAddress2, consulHost, consulPort, 2);

        skinnyBorschEater.registerService();
        fatBorschEater.registerService();

        fatBorschEater.launchBorsch();
        skinnyBorschEater.launchBorsch();
        while (true){

        }
    }
}
