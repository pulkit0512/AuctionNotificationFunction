package com.biddingSystem.cloudFunctions.service.impl;

import com.biddingSystem.cloudFunctions.dao.impl.FirestoreDAOImpl;
import com.biddingSystem.cloudFunctions.dao.impl.SpannerDatastoreDAOImpl;
import com.biddingSystem.cloudFunctions.dto.AuctionData;
import com.biddingSystem.cloudFunctions.service.NotifyWinnerService;
import com.google.common.base.MoreObjects;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class NotifyWinnerServiceImpl implements NotifyWinnerService {
    private static volatile NotifyWinnerServiceImpl notifyWinnerService;
    private NotifyWinnerServiceImpl(){

    }

    public static NotifyWinnerServiceImpl getInstance() {
        if (notifyWinnerService == null) {
            synchronized (NotifyWinnerServiceImpl.class) {
                if (notifyWinnerService == null) {
                    notifyWinnerService = new NotifyWinnerServiceImpl();
                }
            }
        }
        return notifyWinnerService;
    }

    private static final Logger LOGGER = Logger.getLogger(NotifyWinnerServiceImpl.class.getName());
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final String SMTP_HOST = "smtp.office365.com";
    private static final String SMTP_PORT = "587";
    private static final String FROM_EMAIL = "biddingsystemnotifier@outlook.com";
    private static final String EMAIL_PASSWORD = MoreObjects.firstNonNull(System.getenv("EMAIL_PASSWORD"), "password");
    private static final String SUBJECT = "Congratulations, you are the auction WINNER";
    private SpannerDatastoreDAOImpl spannerDatastoreDAO;
    private FirestoreDAOImpl firestoreDAO;
    @Override
    public void notifyWinner(String auctionId, String category) throws Exception {
        LOGGER.info("Notifying winner for auctionId: " + auctionId);
        CompletableFuture<AuctionData> spannerAuctionDataFuture = CompletableFuture.supplyAsync(()->spannerDatastoreDAO.getAuctionData(auctionId));
        CompletableFuture<AuctionData> firestoreAuctionDataFuture = CompletableFuture.supplyAsync(()->firestoreDAO.getAuctionData(auctionId, category));

        AuctionData auctionData = spannerAuctionDataFuture
                .thenCombine(firestoreAuctionDataFuture, (spanner, firestore) -> {
                    firestore.setAuctionId(spanner.getAuctionId());
                    firestore.setWinnerEmail(spanner.getWinnerEmail());
                    firestore.setMaxBidPrice(spanner.getMaxBidPrice());
                    return firestore;
                }).join();

        if (StringUtils.isEmpty(auctionData.getWinnerEmail()) || StringUtils.isEmpty(auctionData.getItemName())) {
            LOGGER.info("No winner for this auction.");
            return;
        }

        sendEmail(auctionData);
    }

    private void sendEmail(AuctionData auctionData) throws Exception {
        LOGGER.info("Sending email.");

        Properties properties = new Properties();
        properties.put("mail.smtp.auth", true);
        properties.put("mail.smtp.starttls.enable", true);
        properties.put("mail.smtp.host", SMTP_HOST);
        properties.put("mail.smtp.port", SMTP_PORT);

        Session session = Session.getInstance(properties, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(FROM_EMAIL, EMAIL_PASSWORD);
            }
        });

        try {
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(FROM_EMAIL));
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(auctionData.getWinnerEmail()));
            message.setSubject(SUBJECT);
            String body = gson.toJson(auctionData);
            message.setText("Congratulations you have won the auction. Below are the details \n\n" + body);

            Transport.send(message);

            LOGGER.info("Email sent successfully!");
        } catch (MessagingException e) {
            LOGGER.info("Error sending email: " + e.getMessage());
            throw new Exception("Error Sending email.");
        }
    }

    public void setSpannerDatastoreDAO(SpannerDatastoreDAOImpl spannerDatastoreDAO) {
        this.spannerDatastoreDAO = spannerDatastoreDAO;
    }

    public void setFirestoreDAO(FirestoreDAOImpl firestoreDAO) {
        this.firestoreDAO = firestoreDAO;
    }
}
