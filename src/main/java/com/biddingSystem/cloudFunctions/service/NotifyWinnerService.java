package com.biddingSystem.cloudFunctions.service;

public interface NotifyWinnerService {
    void notifyWinner(String auctionId, String category) throws Exception;
}
