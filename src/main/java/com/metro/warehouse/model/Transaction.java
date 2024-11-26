package com.metro.warehouse.model;

import java.time.LocalDateTime;

public class Transaction {
    private int orderId;
    private LocalDateTime orderDate;
    private int productId;
    private int customerId;
    private int quantity;
    private int timeId;

    // Constructors
    public Transaction() {}

    public Transaction(int orderId, LocalDateTime orderDate, int productId, int customerId, int quantity, int timeId) {
        this.orderId = orderId;
        this.orderDate = orderDate;
        this.productId = productId;
        this.customerId = customerId;
        this.quantity = quantity;
        this.timeId = timeId;
    }

    // Getters and Setters
    public int getOrderId() { return orderId; }
    public void setOrderId(int orderId) { this.orderId = orderId; }

    public LocalDateTime getOrderDate() { return orderDate; }
    public void setOrderDate(LocalDateTime orderDate) { this.orderDate = orderDate; }

    public int getProductId() { return productId; }
    public void setProductId(int productId) { this.productId = productId; }

    public int getCustomerId() { return customerId; }
    public void setCustomerId(int customerId) { this.customerId = customerId; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public int getTimeId() { return timeId; }
    public void setTimeId(int timeId) { this.timeId = timeId; }
}
