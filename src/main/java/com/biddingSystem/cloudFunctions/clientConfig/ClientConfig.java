package com.biddingSystem.cloudFunctions.clientConfig;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.MoreObjects;

import java.io.IOException;

public class ClientConfig {
    private static volatile ClientConfig clientConfig;
    private ClientConfig() {

    }

    public static ClientConfig getInstance() {
        if (clientConfig == null) {
            synchronized (ClientConfig.class) {
                if (clientConfig == null) {
                    clientConfig = new ClientConfig();
                }
            }
        }
        return clientConfig;
    }

    private static final String PROJECT_NAME =
            MoreObjects.firstNonNull(System.getenv("PROJECT_NAME"), "biddingsystem-411900");
    private static final String INSTANCE_ID =
            MoreObjects.firstNonNull(System.getenv("INSTANCE_ID"), "biddingsystemdb");
    private static final String DATABASE_ID =
            MoreObjects.firstNonNull(System.getenv("DATABASE_ID"), "bidding_system");

    private static DatabaseClient databaseClient;
    private static Firestore firestore;
    private static Storage storage;

    public DatabaseClient databaseClient() throws IOException {
        if (databaseClient == null) {
            Spanner spanner = SpannerOptions.newBuilder()
                    .setProjectId(PROJECT_NAME)
                    .setCredentials(GoogleCredentials.getApplicationDefault())
                    .build()
                    .getService();

            databaseClient = spanner.getDatabaseClient(DatabaseId.of(PROJECT_NAME, INSTANCE_ID, DATABASE_ID));
        }
        return databaseClient;
    }

    public Firestore firestoreClient() throws IOException {
        if (firestore == null) {
            FirestoreOptions firestoreOptions =
                    FirestoreOptions.getDefaultInstance().toBuilder()
                            .setProjectId(PROJECT_NAME)
                            .setCredentials(GoogleCredentials.getApplicationDefault())
                            .build();
            firestore = firestoreOptions.getService();
        }
        return firestore;
    }

    public Storage storageClient() {
        if (storage == null) {
            storage = StorageOptions.newBuilder().setProjectId(PROJECT_NAME).build().getService();
        }
        return storage;
    }
}
