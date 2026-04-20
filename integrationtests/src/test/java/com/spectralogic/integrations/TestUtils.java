package com.spectralogic.integrations;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.commands.*;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.common.Credentials;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;


import static com.spectralogic.integrations.DatabaseUtils.revertBlobSize;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.TestConstants.*;
import static com.spectralogic.integrations.TestConstants.DATA_POLICY_TAPE_SINGLE_COPY_NAME;


public class TestUtils {
    public static Credentials creds;


    public static void cleanupSimulator() throws IOException, InterruptedException {
        HttpClient simClient = HttpClient.newHttpClient();
        HttpRequest simRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:7071/cleanupFiles")) // Replace with your URL
                .GET()
                .build();

        simClient.send(simRequest, HttpResponse.BodyHandlers.ofString());
    }


    public static void cleanupFiles(String path)  {
        try {
            Files.walk(Paths.get(path))
                    .sorted(Comparator.reverseOrder()) // Delete children before parents
                    .map(Path::toFile)
                    .forEach(file -> {
                        if (!file.delete()) {
                            System.err.println("Failed to delete: " + file);
                        }
                    });
        } catch (IOException e) {
            System.err.println("Error deleting directory: " + e.getMessage());
        }
    }

    public static void cleanSetUp(Ds3Client client) throws IOException, InterruptedException, SQLException {
        if (client != null) {
            cleanupAllBuckets(client);
            clearAllJobs(client);
            clearS3ReplicationRules(client);
            clearAzureReplicationRules(client);
            clearAzureTargets(client);
            clearDs3Targets(client);
            clearS3Targets(client);
            reclaimCache(client);
            clearBlobSizes(client);
           // cleanupSimulator();
        }
    }

    public static void clearBlobSizes(Ds3Client client) throws IOException, SQLException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        for (DataPolicy policy: dataPolicies) {
            if (policy.getDefaultBlobSize() != null) {
                revertBlobSize(policy.getName());
            }
        }
    }


    public static Ds3Client setTestParams() {
        String runsOnBP = System.getenv("BP_USED");
        String url =  System.getenv("DS3_ENDPOINT");
        String clientId = System.getenv("DS3_ACCESS_KEY");
        String key = System.getenv("DS3_SECRET_KEY");

        if (runsOnBP != null && runsOnBP.equals("true")) {
            System.out.println("TESTS are run on BP: " + url);
            creds = new Credentials(clientId, key);
            Ds3Client client = Ds3ClientBuilder.fromEnv().withHttps(false).build();
            return client;
        } else {
            creds = new Credentials(authId, secretKey);
            return  Ds3ClientBuilder.create("http://127.0.0.1:8081", creds).build();
        }
    }

}
