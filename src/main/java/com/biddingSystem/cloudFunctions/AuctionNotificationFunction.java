package com.biddingSystem.cloudFunctions;

import com.biddingSystem.cloudFunctions.clientConfig.ClientConfig;
import com.biddingSystem.cloudFunctions.dao.impl.FirestoreDAOImpl;
import com.biddingSystem.cloudFunctions.dao.impl.SpannerDatastoreDAOImpl;
import com.biddingSystem.cloudFunctions.service.impl.NotifyWinnerServiceImpl;
import com.biddingSystem.cloudFunctions.service.impl.PostAuctionServiceImpl;
import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.util.logging.Logger;

public class AuctionNotificationFunction implements HttpFunction {
    private static final Logger LOGGER = Logger.getLogger(AuctionNotificationFunction.class.getName());
    private static final Gson gson = new Gson();

    private volatile NotifyWinnerServiceImpl notifyWinnerService;
    private volatile PostAuctionServiceImpl postAuctionService;

    public void service(final HttpRequest request, final HttpResponse response) throws Exception {
        createServiceObjects();

        String payload = request.getReader().readLine();
        JsonObject jsonObject = gson.fromJson(payload, JsonObject.class);
        String auctionId = jsonObject.get("auctionId").getAsString();
        String category = jsonObject.get("category").getAsString();
        LOGGER.info("Task triggered for Auction Id: " + auctionId + ", Category: " + category);

        notifyWinnerService.notifyWinner(auctionId, category);
        postAuctionService.archiveAuctionData(auctionId, category);
        postAuctionService.cleanUpAuctionData(auctionId, category);
    }

    private void createServiceObjects() throws IOException {
        if (notifyWinnerService == null) {
            synchronized (AuctionNotificationFunction.class) {
                if (notifyWinnerService == null) {
                    createNotifyServiceObject();
                }
            }
        }

        if (postAuctionService == null) {
            synchronized (AuctionNotificationFunction.class) {
                if (postAuctionService == null) {
                    createPostAuctionServiceObject();
                }
            }
        }
    }

    private void createNotifyServiceObject() throws IOException {
        notifyWinnerService = NotifyWinnerServiceImpl.getInstance();

        SpannerDatastoreDAOImpl spannerDatastoreDAO = SpannerDatastoreDAOImpl.getInstance();
        spannerDatastoreDAO.setDatabaseClient(ClientConfig.getInstance().databaseClient());
        notifyWinnerService.setSpannerDatastoreDAO(spannerDatastoreDAO);

        FirestoreDAOImpl firestoreDAO = FirestoreDAOImpl.getInstance();
        firestoreDAO.setFirestoreClient(ClientConfig.getInstance().firestoreClient());
        notifyWinnerService.setFirestoreDAO(firestoreDAO);
    }

    private void createPostAuctionServiceObject() throws IOException {
        postAuctionService = PostAuctionServiceImpl.getInstance();

        SpannerDatastoreDAOImpl spannerDatastoreDAO = SpannerDatastoreDAOImpl.getInstance();
        spannerDatastoreDAO.setDatabaseClient(ClientConfig.getInstance().databaseClient());
        postAuctionService.setSpannerDatastoreDAO(spannerDatastoreDAO);

        FirestoreDAOImpl firestoreDAO = FirestoreDAOImpl.getInstance();
        firestoreDAO.setFirestoreClient(ClientConfig.getInstance().firestoreClient());
        postAuctionService.setFirestoreDAO(firestoreDAO);

        postAuctionService.setStorage(ClientConfig.getInstance().storageClient());
    }

    public void setNotifyWinnerService(NotifyWinnerServiceImpl notifyWinnerService) {
        this.notifyWinnerService = notifyWinnerService;
    }

    public void setPostAuctionService(PostAuctionServiceImpl postAuctionService) {
        this.postAuctionService = postAuctionService;
    }
}
