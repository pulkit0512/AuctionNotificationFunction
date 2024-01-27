package com.biddingSystem.cloudFunctions.dao.impl;

import com.biddingSystem.cloudFunctions.dao.FirestoreDAO;
import com.biddingSystem.cloudFunctions.dto.AuctionData;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;

import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class FirestoreDAOImpl implements FirestoreDAO {

    private static volatile FirestoreDAOImpl firestoreDAO;
    private FirestoreDAOImpl() {

    }

    public static FirestoreDAOImpl getInstance() {
        if (firestoreDAO == null) {
            synchronized (FirestoreDAOImpl.class) {
                if (firestoreDAO == null) {
                    firestoreDAO = new FirestoreDAOImpl();
                }
            }
        }
        return firestoreDAO;
    }

    private static final Logger LOGGER = Logger.getLogger(FirestoreDAOImpl.class.getName());
    private Firestore firestoreClient;

    @Override
    public AuctionData getAuctionData(String auctionId, String category) {
        LOGGER.info("Inside Firestore DAO to read auction details for category: " + category + ", auctionId: " + auctionId);
        DocumentReference docRef = firestoreClient.collection(category).document(auctionId);
        // asynchronously retrieve the document
        ApiFuture<DocumentSnapshot> future = docRef.get();

        // block on response
        DocumentSnapshot document;
        try {
            document = future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        AuctionData auctionData;
        if (document.exists()) {
            auctionData = document.toObject(AuctionData.class);
            if (auctionData != null) {
                auctionData.setAuctionId(auctionId);
            }
        } else {
            auctionData = new AuctionData();
        }
        return auctionData;
    }

    @Override
    public void deleteDocument(String auctionId, String category) {
        LOGGER.info("Cleaning up firestore data");
        firestoreClient.collection(category).document(auctionId).delete();
    }

    public void setFirestoreClient(Firestore firestoreClient) {
        this.firestoreClient = firestoreClient;
    }
}
