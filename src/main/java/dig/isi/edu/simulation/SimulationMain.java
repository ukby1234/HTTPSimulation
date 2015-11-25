package dig.isi.edu.simulation;

import dig.isi.edu.simulation.worker.ClientWorker;
import org.apache.commons.cli.*;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.FileInputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by chengyey on 11/24/15.
 */
public class SimulationMain {
    public static void main(String[] args) throws Exception {
        Options options = createCommandLineOptions();
        CommandLineParser parser = new BasicParser();
        CommandLine cl = null;
        cl = parser.parse(options, args);
        if (cl == null || cl.getOptions().length == 0 || cl.hasOption("help") || !cl.hasOption("config")) {
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp(SimulationMain.class.getSimpleName(), options);
            return;
        }
        String configPath = cl.getOptionValue("config");
        String outputPath = cl.getOptionValue("output", "");
        JSONObject config = new JSONObject(new JSONTokener(new FileInputStream(configPath)));
        Double timeout = config.getDouble("timeout");
        Double rate = config.getDouble("rate");
        JSONArray clientArray = config.getJSONArray("clients");
        List<List<String>> clientConfigs = parseClientConfig(clientArray);
        if (!cl.hasOption("onetime")) {
            List<Client> clients = new ArrayList<>();
            int count = 0;
            for (List<String> queries : clientConfigs) {
                Client client = new Client("client-" + (count++), outputPath, queries, rate);
                clients.add(client);
                Thread thread = new Thread(client);
                thread.start();
            }
            Thread.sleep((long) (timeout * 1000));
            for (Client client : clients) {
                client.kill();
            }
        }
        else {
            int count = 0;
            CloseableHttpAsyncClient client = HttpAsyncClients.createDefault();
            client.start();
            for (List<String> queries : clientConfigs) {
                PrintWriter pw = new PrintWriter(outputPath + "client-" + (count) + ".csv");
                ClientWorker worker = new ClientWorker(queries, client);
                Map<String, Long> result = worker.call();
                for (Map.Entry<String, Long> entry : result.entrySet()) {
                    pw.println(entry.getKey() + "," + entry.getValue());
                }
                pw.close();
                System.out.println("client-" + (count++) +  "-Max: " + Collections.max(result.values()));
            }
            client.close();
        }
    }

    private static Options createCommandLineOptions() {
        Options options = new Options();
        options.addOption(new Option("config", "config", true, "Path to config file"));
        options.addOption(new Option("output", "output", true, "Directory to output"));
        options.addOption(new Option("onetime", "onetime", false, "One time execution"));
        return options;
    }

    private static List<List<String>> parseClientConfig(JSONArray clientArray) {
        List<List<String>> clients = new ArrayList<>();
        for (int i = 0; i < clientArray.length(); i++) {
            JSONObject clientObject = clientArray.getJSONObject(i);
            JSONArray queryArray = clientObject.getJSONArray("queries");
            List<String> queries = new ArrayList<>();
            for (int j = 0; j < queryArray.length(); j++) {
                queries.add(queryArray.getString(j));
            }
            clients.add(queries);
        }
        return clients;
    }
}
