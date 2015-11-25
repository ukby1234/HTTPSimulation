package dig.isi.edu.simulation.worker;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/**
 * Created by chengyey on 11/24/15.
 */
public class ClientCollector implements Runnable{
    private ConcurrentLinkedQueue<FutureTask<Map<String, Long>>> workers;
    private long awaitTime = 0;
    private long count = 0;
    private boolean isKilled = false;
    PrintWriter pw;
    public ClientCollector(String clientId, String outputPath, ConcurrentLinkedQueue<FutureTask<Map<String, Long>>> workers) {
        this.workers = workers;
        try {
            pw = new PrintWriter(outputPath + clientId + ".csv");
        } catch (Exception e) {

        }
    }

    public void kill() {
        isKilled = true;
    }
    @Override
    public void run() {
        while(!isKilled) {
            collect();
        }
    }

    public void collect() {
        Iterator<FutureTask<Map<String, Long>>> iterator = workers.iterator();
        while (iterator.hasNext()) {
            FutureTask<Map<String, Long>> worker = iterator.next();
            try {
                Map<String, Long> result;
                if (!isKilled) {
                    result = worker.get(100, TimeUnit.MILLISECONDS);
                } else {
                    result = worker.get();
                }
                if (pw != null) {
                    for (Map.Entry<String, Long> entry : result.entrySet()) {
                        pw.println(count + "," + entry.getKey() + "," + entry.getValue());
                    }
                    pw.flush();
                }
                long max = Collections.max(result.values());
                if (max == Long.MAX_VALUE) {
                    awaitTime = Long.MAX_VALUE;
                } else if (awaitTime != Long.MAX_VALUE){
                    awaitTime += max;
                }
                count++;
                iterator.remove();
            } catch (Exception e) {

            }
        }
    }

    public long getCount() {
        return count;
    }

    public long getAwaitTime() {
        return awaitTime;
    }

    public void closeFile() {
        if (pw != null) {
            pw.close();
        }
    }
}
