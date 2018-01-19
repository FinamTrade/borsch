package ru.finam.borsch.partitioner;

import ru.finam.borsch.HostPortAddress;

import java.util.HashSet;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.*;

/**
 * Created by akhaymovich on 19.01.18.
 */
public class HashingRingTest {

    private static SortedSet<HostPortAddress> createHostSet() {
        SortedSet<HostPortAddress> set = new TreeSet<>();
        HostPortAddress hostPortAddress1 = new HostPortAddress("127.0.0.1", 90);
        HostPortAddress hostPortAddress2 = new HostPortAddress("127.0.0.1", 92);
        HostPortAddress hostPortAddress3 = new HostPortAddress("127.0.0.1", 94);
        HostPortAddress hostPortAddress4 = new HostPortAddress("127.0.0.1", 91);
        HostPortAddress hostPortAddress5 = new HostPortAddress("127.0.0.1", 93);
        HostPortAddress hostPortAddress6 = new HostPortAddress("127.0.0.1", 95);

        set.add(hostPortAddress1);
        set.add(hostPortAddress2);
        set.add(hostPortAddress3);
        set.add(hostPortAddress4);
        set.add(hostPortAddress5);
        set.add(hostPortAddress6);
        return set;
    }

    public static void main(String[] args) {

        HashingRing hashingRing = new HashingRing(createHostSet());
       assertEquals(new HostPortAddress("127.0.0.1", 90), hashingRing.getServer("account1"));


    }


}