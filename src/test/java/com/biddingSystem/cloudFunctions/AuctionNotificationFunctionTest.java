package com.biddingSystem.cloudFunctions;

import com.biddingSystem.cloudFunctions.clientConfig.ClientConfig;
import com.biddingSystem.cloudFunctions.dto.AuctionData;
import com.biddingSystem.cloudFunctions.service.impl.NotifyWinnerServiceImpl;
import com.google.cloud.Timestamp;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.testing.TestLogHandler;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.logging.Logger;

import static com.google.common.truth.Truth.assertThat;

public class AuctionNotificationFunctionTest {
    private static final Logger FUNCTION_LOGGER = Logger.getLogger(AuctionNotificationFunction.class.getName());
    private static final Logger NOTIFY_LOGGER = Logger.getLogger(NotifyWinnerServiceImpl.class.getName());

    private static final Gson gson = new Gson();

    @Test
    public void serviceTest() throws Exception {
        insertDataForTesting();
        AuctionNotificationFunction auctionNotificationFunction = new AuctionNotificationFunction();
        TestLogHandler FUNCTION_LOG_HANDLER = new TestLogHandler();
        TestLogHandler NOTIFY_LOG_HANDLER = new TestLogHandler();
        FUNCTION_LOGGER.addHandler(FUNCTION_LOG_HANDLER);
        NOTIFY_LOGGER.addHandler(NOTIFY_LOG_HANDLER);

        HttpRequest httpRequest = Mockito.mock(HttpRequest.class);
        HttpResponse httpResponse = Mockito.mock(HttpResponse.class);

        BufferedReader reader = Mockito.mock(BufferedReader.class);

        Mockito.when(httpRequest.getReader()).thenReturn(reader);
        Mockito.when(reader.readLine()).thenReturn("{\"auctionId\":\"01920b11\", \"category\":\"Car\"}");

        auctionNotificationFunction.service(httpRequest, httpResponse);
        assertThat(FUNCTION_LOG_HANDLER.getStoredLogRecords().get(0).getMessage()).isEqualTo("Task triggered for Auction Id: 01920b11, Category: Car");
        assertThat(NOTIFY_LOG_HANDLER.getStoredLogRecords().get(1).getMessage()).isEqualTo("No winner for this auction.");

        Storage storage = ClientConfig.getInstance().storageClient();
        String staticPath = "archive/STATIC/01920b11.json";
        String auctionPath = "archive/AUCTION/01920b11_AUCTION.csv";
        String bidPath = "archive/BID/01920b11_BID.csv";
        String bucketName = "bidding_system_bucket";

        BlobId staticBlobId = BlobId.of(bucketName, staticPath);
        Blob staticBlob = storage.get(staticBlobId);
        Assert.assertNotNull(staticBlob);
        storage.get(staticBlobId).delete();

        BlobId auctionBlobId = BlobId.of(bucketName, auctionPath);
        Blob auctionBlob = storage.get(auctionBlobId);
        Assert.assertNotNull(auctionBlob);
        storage.get(auctionBlobId).delete();

        BlobId bidBlobId = BlobId.of(bucketName, bidPath);
        Blob bidBlob = storage.get(bidBlobId);
        Assert.assertNotNull(bidBlob);
        storage.get(bidBlobId).delete();
    }

    private void insertDataForTesting() throws IOException {
        AuctionData auctionData = getAuctionData();
        auctionData.setExpirationTime(LocalDateTime.now().plusDays(4).toString());
        auctionData.setExpirationInSeconds(LocalDateTime.now().plusDays(4).toEpochSecond(ZoneOffset.UTC));


        DatabaseClient databaseClient = ClientConfig.getInstance().databaseClient();
        Mutation mutation = Mutation.newInsertBuilder("AUCTION")
                .set("AUCTION_ID").to("01920b11")
                .set("CATEGORY").to("Car")
                .set("BASE_PRICE").to(1400.00)
                .set("MAX_BID_PRICE").to(1500.00)
                .set("AUCTION_CREATION_TIME").to(Timestamp.now())
                .set("AUCTION_EXPIRY_TIME").to(Timestamp.ofTimeSecondsAndNanos(LocalDateTime.now().plusDays(4).toEpochSecond(ZoneOffset.UTC), 0))
                .build();
        databaseClient.write(Collections.singleton(mutation));

        Mutation mutation1 = Mutation.newInsertBuilder("BID")
                .set("AUCTION_ID").to("01920b11")
                .set("C_USER_ID").to("d90007c2-562a-447a-af38-85d754077fa0")
                .set("MAX_BID_PRICE").to(1500.00)
                .set("BID_TIME").to(Timestamp.now())
                .build();
        databaseClient.write(Collections.singleton(mutation1));

        ClientConfig.getInstance().firestoreClient().collection(auctionData.getItemCategory()).document("01920b11").set(auctionData);
    }

    private AuctionData getAuctionData() {
        String data = "{\"itemCategory\" : \"Car\", \"itemName\" : \"BMW A6\", \"basePrice\" : \"1400\", \"currencyCode\" : \"USD\", \"expirationTime\" : \"2024-02-05T11:50:55\", \"itemAttributes\" : {\"yearOfPurchase\" : \"2022\", \"distanceTravelled\" : \"270KM\", \"color\" : \"Black\"}}";

        return gson.fromJson(data, AuctionData.class);
    }
}