package edu.isi.simulation.worker;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.nio.client.HttpAsyncClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

/**
 * Created by chengyey on 11/24/15.
 */
public class ClientWorker implements Callable<Map<String, Long>>{
    private List<String> queries;
    HttpAsyncClient client;
    public ClientWorker(List<String> queries, HttpAsyncClient client) {
        this.queries = queries;
        this.client = client;
    }
    public Map<String, Long> call() throws Exception {
        final Map<String, Long> executionTime = new HashMap<>();
        final long start = System.currentTimeMillis();
        final CountDownLatch latch = new CountDownLatch(queries.size());
        for (final String url : queries) {
            client.execute(new HttpGet(url), new FutureCallback<HttpResponse>() {
                public void completed(HttpResponse httpResponse) {
                    latch.countDown();
                    long end = System.currentTimeMillis();
                    executionTime.put(url, end - start);
                }

                public void failed(Exception e) {
                    latch.countDown();
                    executionTime.put(url, Long.MAX_VALUE);
                }

                public void cancelled() {
                    latch.countDown();
                    executionTime.put(url, Long.MAX_VALUE);
                }
            });
        }
        latch.await();
        return executionTime;
    }
}
