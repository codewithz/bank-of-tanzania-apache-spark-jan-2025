package com.codewithz.model;

public class Transaction {

    private String txId;
    private String bankName;
    private String accountHolder;
    private double amount;
    private double balanceAmount;
    private String country;
    private String state;
    private long timestamp;
    private int txTypeCode;

    public Transaction() {
    }

    public Transaction(String txId, String bankName, String accountHolder, double amount, double balanceAmount, String country, String state, long timestamp, int txTypeCode) {
        this.txId = txId;
        this.bankName = bankName;
        this.accountHolder = accountHolder;
        this.amount = amount;
        this.balanceAmount = balanceAmount;
        this.country = country;
        this.state = state;
        this.timestamp = timestamp;
        this.txTypeCode = txTypeCode;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getBankName() {
        return bankName;
    }

    public void setBankName(String bankName) {
        this.bankName = bankName;
    }

    public String getAccountHolder() {
        return accountHolder;
    }

    public void setAccountHolder(String accountHolder) {
        this.accountHolder = accountHolder;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public double getBalanceAmount() {
        return balanceAmount;
    }

    public void setBalanceAmount(double balanceAmount) {
        this.balanceAmount = balanceAmount;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getTxTypeCode() {
        return txTypeCode;
    }

    public void setTxTypeCode(int txTypeCode) {
        this.txTypeCode = txTypeCode;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "txId='" + txId + '\'' +
                ", bankName='" + bankName + '\'' +
                ", accountHolder='" + accountHolder + '\'' +
                ", amount=" + amount +
                ", balanceAmount=" + balanceAmount +
                ", country='" + country + '\'' +
                ", state='" + state + '\'' +
                ", timestamp=" + timestamp +
                ", txTypeCode=" + txTypeCode +
                '}';
    }
}
