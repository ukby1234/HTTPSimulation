package dig.isi.edu.simulation;

import dig.isi.edu.simulation.worker.ClientCollector;
import dig.isi.edu.simulation.worker.ClientWorker;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

/**
 * Created by chengyey on 11/24/15.
 */
public class Client implements Runnable{
    private boolean kill;
    private List<String> queries;
    private double rate;
    private String clientId;
    private ConcurrentLinkedQueue<FutureTask<Map<String, Long>>> workers;
    private ClientCollector collector;
    ExecutorService executor = Executors.newFixedThreadPool(20);
    public Client(String clientId, String outputPath, List<String> queries, double rate) {
        this.queries = queries;
        this.rate = rate;
        this.clientId = clientId;
        workers = new ConcurrentLinkedQueue<>();
        collector = new ClientCollector(clientId, outputPath, workers);
    }

    public void kill() {
        kill = true;
        collector.kill();
    }
    public void run() {
        RandomDataGenerator generator = new RandomDataGenerator();
        CloseableHttpAsyncClient client = HttpAsyncClients.createDefault();
        Thread collectorThread = new Thread(collector);
        collectorThread.start();
        client.start();
        while(!kill) {
            double next = generator.nextExponential(1.0 / rate);
            try {
                Thread.sleep((long) (next * 1000));
                FutureTask<Map<String, Long>> task = new FutureTask<>(new ClientWorker(queries, client));
                executor.execute(task);
                workers.add(task);
            } catch (Exception e) {

            }
        }
        collector.collect();
        collector.closeFile();
        executor.shutdown();
        try {
            client.close();
        } catch (IOException e) {

        }
        System.out.println(clientId + " Average: " + collector.getAwaitTime() * 1.0 / collector.getCount());
    }
}
