package com.biddingSystem.cloudFunctions.service.impl;

import com.biddingSystem.cloudFunctions.dao.impl.FirestoreDAOImpl;
import com.biddingSystem.cloudFunctions.dao.impl.SpannerDatastoreDAOImpl;
import com.biddingSystem.cloudFunctions.dto.AuctionData;
import com.biddingSystem.cloudFunctions.service.PostAuctionService;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.base.MoreObjects;
import com.google.gson.Gson;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class PostAuctionServiceImpl implements PostAuctionService {
    private static volatile PostAuctionServiceImpl postAuctionService;
    private PostAuctionServiceImpl(){

    }

    public static PostAuctionServiceImpl getInstance() {
        if (postAuctionService == null) {
            synchronized (PostAuctionServiceImpl.class) {
                if (postAuctionService == null) {
                    postAuctionService = new PostAuctionServiceImpl();
                }
            }
        }
        return postAuctionService;
    }

    private static final Logger LOGGER = Logger.getLogger(PostAuctionServiceImpl.class.getName());
    private static final Gson gson = new Gson();
    private static final String BUCKET_NAME = MoreObjects.firstNonNull(System.getenv("BUCKET_NAME"), "bidding_system_bucket");

    private SpannerDatastoreDAOImpl spannerDatastoreDAO;
    private FirestoreDAOImpl firestoreDAO;
    private Storage storage;

    @Override
    public void cleanUpAuctionData(String auctionId, String category) {
        LOGGER.info("CleanUp of auction data started.");
        CompletableFuture<Void> spannerCleanUp = CompletableFuture.runAsync(() -> spannerDatastoreDAO.cleanData(auctionId));
        CompletableFuture<Void> firestoreCleanUp = CompletableFuture.runAsync(() -> firestoreDAO.deleteDocument(auctionId, category));

        CompletableFuture.allOf(spannerCleanUp, firestoreCleanUp).join();
        LOGGER.info("CleanUp of auction data completed.");
    }

    @Override
    public void archiveAuctionData(String auctionId, String category) {
        LOGGER.info("Archiving auction data started.");
        CompletableFuture<Void> spannerArchival = CompletableFuture.runAsync(()->spannerDatastoreDAO.archiveData(auctionId));

        CompletableFuture<Void> firestoreArchival = CompletableFuture.supplyAsync(() -> firestoreDAO.getAuctionData(auctionId, category))
                .thenAccept(this::pushStaticAuctionDataToGcs);

        CompletableFuture.allOf(firestoreArchival, spannerArchival).join();

        CompletableFuture<Void> pushBidToGcs = CompletableFuture.runAsync(() -> writeToGcsBucket(auctionId, "BID"));
        CompletableFuture<Void> pushAuctionToGcs = CompletableFuture.runAsync(() -> writeToGcsBucket(auctionId, "AUCTION"));
        CompletableFuture.allOf(pushBidToGcs, pushAuctionToGcs).join();
        LOGGER.info("Archiving auction data completed.");
    }

    private void pushStaticAuctionDataToGcs(AuctionData auctionData) {
        LOGGER.info("Writing static data to GCS bucket");
        String fileName = auctionData.getAuctionId()+".json";
        String data = gson.toJson(auctionData);
        String bucketPath = "archive/STATIC/" + fileName;
        byte[] content = data.getBytes();
        BlobId blobId = BlobId.of(BUCKET_NAME, bucketPath);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo, content);
        LOGGER.info("Static data pushed successfully, " + fileName);
    }

    private void writeToGcsBucket(String auctionId, String type) {
        LOGGER.info("Writing Archived files to GCS Bucket");
        try {
            String fileName = auctionId + "_" + type + ".csv";
            String filePath = "/tmp/" + fileName;
            String bucketPath = "archive/" + type + "/" + fileName;
            File file = new File(filePath);
            if (file.exists()) {
                byte[] content = Files.readAllBytes(Paths.get(filePath));
                BlobId blobId = BlobId.of(BUCKET_NAME, bucketPath);
                BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                storage.create(blobInfo, content);
                Files.delete(file.toPath());
                LOGGER.info("File push successfully, " + fileName);
            }
        } catch (IOException ex) {
            LOGGER.warning("Unable to write to GCS bucket.");
        }
    }

    public void setSpannerDatastoreDAO(SpannerDatastoreDAOImpl spannerDatastoreDAO) {
        this.spannerDatastoreDAO = spannerDatastoreDAO;
    }

    public void setFirestoreDAO(FirestoreDAOImpl firestoreDAO) {
        this.firestoreDAO = firestoreDAO;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }
}
