package com.finance.lab.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * 金融动账流事件核心领域实体 (FinancialTxEvent)
 * 标准 Flink POJO，支持无界流极致高效序列化。
 */
public class FinancialTxEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String txId;               // 交易流水号 (如 'TX-001')
    private String cardTail;           // 银行卡尾号 (用于 keyBy，如 '3342')
    private Instant eventTime;         // 业务发生时间戳 (用于 Event Time 与 Watermark 提取)
    private BigDecimal amount;         // 动账金额
    private String merchant;           // 纯净商户名 (如 '美团', '盒马鲜生')
    private String category;           // 消费大类 (如 'FOOD', 'TRANSPORT')

    public FinancialTxEvent() {
    }

    public FinancialTxEvent(String txId, String cardTail, Instant eventTime,
                            BigDecimal amount, String merchant, String category) {
        this.txId = txId;
        this.cardTail = cardTail;
        this.eventTime = eventTime;
        this.amount = amount;
        this.merchant = merchant;
        this.category = category;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getCardTail() {
        return cardTail;
    }

    public void setCardTail(String cardTail) {
        this.cardTail = cardTail;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public void setEventTime(Instant eventTime) {
        this.eventTime = eventTime;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getMerchant() {
        return merchant;
    }

    public void setMerchant(String merchant) {
        this.merchant = merchant;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FinancialTxEvent that = (FinancialTxEvent) o;
        return Objects.equals(txId, that.txId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(txId);
    }

    @Override
    public String toString() {
        return "FinancialTxEvent{" +
                "txId='" + txId + '\'' +
                ", cardTail='" + cardTail + '\'' +
                ", eventTime=" + eventTime +
                ", amount=" + amount +
                ", merchant='" + merchant + '\'' +
                ", category='" + category + '\'' +
                '}';
    }
}
