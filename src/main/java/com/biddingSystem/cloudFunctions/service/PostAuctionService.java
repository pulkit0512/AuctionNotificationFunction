package com.biddingSystem.cloudFunctions.service;

public interface PostAuctionService {
    void cleanUpAuctionData(String auctionId, String category);
    void archiveAuctionData(String auctionId, String category);
}
