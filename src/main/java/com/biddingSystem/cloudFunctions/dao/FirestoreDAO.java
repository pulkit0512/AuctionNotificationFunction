package com.biddingSystem.cloudFunctions.dao;

import com.biddingSystem.cloudFunctions.dto.AuctionData;

public interface FirestoreDAO {
    AuctionData getAuctionData(String auctionId, String category);
    void deleteDocument(String auctionId, String category);
}
