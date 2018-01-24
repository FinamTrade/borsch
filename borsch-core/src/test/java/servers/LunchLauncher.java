package servers;

import ru.finam.borsch.HostPortAddress;

/**
 * Created by akhaymovich on 08.10.17.
 */
public class LunchLauncher {

    public static void main(String[] args) {
        HostPortAddress grpcAddress1 = new HostPortAddress("localhost", 50101, 50101);
        HostPortAddress grpcAddress2 = new HostPortAddress("localhost", 50102, 50102);

        FatBorschEater fatBorschEater =
                new FatBorschEater(grpcAddress1, 2);

        SkinnyBorschEater skinnyBorschEater =
                new SkinnyBorschEater(grpcAddress2,  1);

        skinnyBorschEater.registerService();
        fatBorschEater.registerService();

        fatBorschEater.launchBorsch();
        skinnyBorschEater.launchBorsch();
        while (true){

        }
    }
}
