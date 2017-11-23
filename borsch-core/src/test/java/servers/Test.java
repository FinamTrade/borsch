package servers;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * Created by akhaymovich on 23.11.17.
 */
public class Test {

    public static void main(String[] args) {

        final CompletableFuture<String> completableFuture = new CompletableFuture<>();



        CompletableFuture<Void> future = CompletableFuture.completedFuture("adxasas").runAsync(new Runnable() {
            @Override
            public void run() {
                System.out.println("first");
            }
        }).thenRun(new Runnable() {
            @Override
            public void run() {
                System.out.println("second");
            }
        }).thenRun(new Runnable() {
            @Override
            public void run() {
                System.out.println("third");
            }
        }).toCompletableFuture();



    }
}
