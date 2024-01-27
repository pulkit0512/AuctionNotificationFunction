package com.biddingSystem.cloudFunctions.dao;

import com.biddingSystem.cloudFunctions.dto.AuctionData;

public interface SpannerDatastoreDAO {
    AuctionData getAuctionData(String auctionId);
    void archiveData(String auctionId);
    void cleanData(String auctionId);
}
