package com.biddingSystem.cloudFunctions.dao.impl;

import com.biddingSystem.cloudFunctions.dao.SpannerDatastoreDAO;
import com.biddingSystem.cloudFunctions.dto.AuctionData;
import com.google.cloud.spanner.*;
import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SpannerDatastoreDAOImpl implements SpannerDatastoreDAO {

    private static volatile SpannerDatastoreDAOImpl spannerDatastoreDAO;
    private SpannerDatastoreDAOImpl() {

    }

    public static SpannerDatastoreDAOImpl getInstance() {
        if (spannerDatastoreDAO == null) {
            synchronized (SpannerDatastoreDAOImpl.class) {
                if (spannerDatastoreDAO == null) {
                    spannerDatastoreDAO = new SpannerDatastoreDAOImpl();
                }
            }
        }
        return spannerDatastoreDAO;
    }

    private static final Logger LOGGER = Logger.getLogger(SpannerDatastoreDAOImpl.class.getName());
    private static final String AUCTION_ID = "auctionId";

    private static final String DELETE_AUCTION_SQL = "DELETE FROM AUCTION WHERE AUCTION_ID = @auctionId";
    private static final String DELETE_BID_SQL = "DELETE FROM BID WHERE AUCTION_ID = @auctionId";
    private static final String ARCHIVE_BID_SQL = "SELECT * FROM BID WHERE AUCTION_ID = @auctionId";
    private static final String ARCHIVE_AUCTION_SQL = "SELECT * FROM AUCTION WHERE AUCTION_ID = @auctionId";
    private static final String READ_AUCTION_SQL = "SELECT A.MAX_BID_PRICE, C.EMAIL " +
            "FROM AUCTION A JOIN C_USER C ON A.C_USER_ID = C.C_USER_ID WHERE AUCTION_ID = @auctionId";

    private DatabaseClient databaseClient;

    @Override
    public AuctionData getAuctionData(String auctionId) {
        LOGGER.info("Inside Spanner DAO to read auction details for auctionId: " + auctionId);
        Statement statement = getStatement(auctionId, READ_AUCTION_SQL);

        AuctionData auctionData = new AuctionData();
        try (ResultSet resultSet = databaseClient.singleUse().executeQuery(statement)) {
            if (resultSet.next()) {
                auctionData.setAuctionId(auctionId);
                Value maxBidPriceValue = resultSet.getValue("MAX_BID_PRICE");
                if (!maxBidPriceValue.isNull()) {
                    auctionData.setMaxBidPrice(maxBidPriceValue.getFloat64());
                }
                Value winnerEmail = resultSet.getValue("EMAIL");
                if (!winnerEmail.isNull()) {
                    auctionData.setWinnerEmail(winnerEmail.getString());
                }
            }
        }
        return auctionData;
    }

    @Override
    public void archiveData(String auctionId) {
        LOGGER.info("Inside SpannerDao, archiving data.");
        CompletableFuture<Void> bidArchival = CompletableFuture.runAsync(()->archiveBidData(auctionId));
        CompletableFuture<Void> auctionArchival = CompletableFuture.runAsync(()->archiveAuctionData(auctionId));

        CompletableFuture.allOf(bidArchival, auctionArchival).join();
    }

    private void archiveBidData(String auctionId) {
        String fileName = auctionId + "_BID.csv";
        String csvFileName = "/tmp/"+fileName;

        Statement archivalStatement = getStatement(auctionId, ARCHIVE_BID_SQL);

        try {
            ResultSet resultSet = databaseClient.singleUse().executeQuery(archivalStatement);
            CSVWriter writer = new CSVWriter(new FileWriter(csvFileName));
            String[] columnNames = new String[]{"BID_ID", "AUCTION_ID", "C_USER_ID", "MAX_BID_PRICE", "BID_TIME"};
            writer.writeNext(columnNames);

            while (resultSet.next()) {
                String[] data = new String[5];
                data[0] = resultSet.getString("BID_ID");
                data[1] = resultSet.getString("AUCTION_ID");
                data[2] = resultSet.getString("C_USER_ID");
                data[3] = String.valueOf(resultSet.getDouble("MAX_BID_PRICE"));
                data[4] = resultSet.getTimestamp("BID_TIME").toString();
                writer.writeNext(data);
            }

            writer.flush();
            LOGGER.info("File created successfully, " + fileName);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Unable to create CSV file: " + fileName);
        }
    }

    private void archiveAuctionData(String auctionId) {
        String fileName = auctionId + "_AUCTION.csv";
        String csvFileName = "/tmp/"+fileName;

        Statement archivalStatement = getStatement(auctionId, ARCHIVE_AUCTION_SQL);

        try {
            ResultSet resultSet = databaseClient.singleUse().executeQuery(archivalStatement);
            CSVWriter writer = new CSVWriter(new FileWriter(csvFileName));
            String[] columnNames = new String[]{"AUCTION_ID", "CATEGORY", "BASE_PRICE", "MAX_BID_PRICE", "C_USER_ID",
                    "AUCTION_CREATION_TIME", "AUCTION_EXPIRY_TIME"};
            writer.writeNext(columnNames);

            while (resultSet.next()) {
                String[] data = new String[7];
                data[0] = resultSet.getString("AUCTION_ID");
                data[1] = resultSet.getString("CATEGORY");
                data[2] = String.valueOf(resultSet.getDouble("BASE_PRICE"));
                Value maxBidPriceValue = resultSet.getValue("MAX_BID_PRICE");
                if (!maxBidPriceValue.isNull()) {
                    data[3] = String.valueOf(maxBidPriceValue.getFloat64());
                }
                Value userIdValue = resultSet.getValue("C_USER_ID");
                if (!userIdValue.isNull()) {
                    data[4] = userIdValue.getString();
                }
                data[5] = resultSet.getTimestamp("AUCTION_CREATION_TIME").toString();
                data[6] = resultSet.getTimestamp("AUCTION_EXPIRY_TIME").toString();
                writer.writeNext(data);
            }

            writer.flush();
            LOGGER.info("File created successfully, " + fileName);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Unable to create CSV file: " + fileName);
        }
    }

    @Override
    public void cleanData(String auctionId) {
        databaseClient.readWriteTransaction()
                .run(transaction -> {
                    Statement bidDeleteStatement = getStatement(auctionId, DELETE_BID_SQL);
                    transaction.executeUpdate(bidDeleteStatement);
                    LOGGER.info("Bid Records deleted.");

                    Statement auctionDeleteStatement = getStatement(auctionId, DELETE_AUCTION_SQL);
                    transaction.executeUpdate(auctionDeleteStatement);
                    LOGGER.info("Auction Record deleted");
                    return null;
                });
    }

    private Statement getStatement(String auctionId, String sql) {
        return Statement.newBuilder(sql)
                .bind(AUCTION_ID)
                .to(auctionId)
                .build();
    }

    public void setDatabaseClient(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }
}
