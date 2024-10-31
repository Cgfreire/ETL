"""CREATE SCHEMA IF NOT EXISTS dw  
CREATE TABLE IF NOT EXISTS dw.sales_orders(
        salesOrderId varchar(256) NULL,  
        orderDate varchar(256) NULL, 
        dueDate varchar(256) NULL, 
        shipDate varchar(256) NULL,
        [status] varchar(256) NULL, 
        isOrderedOnline varchar(256) NULL, 
        salesOrderNumber varchar(256) NULL, 
        purchaseOrderNumber varchar(256) NULL,
        accountNumber varchar(256) NULL, 
        customerId varchar(256) NULL, 
        salesPersonId varchar(256) NULL, 
        territory varchar(256) NULL,
        billToAddress varchar(256) NULL, 
        shipToAddress varchar(256) NULL, 
        shipMethod varchar(256) NULL, 
        creditCardId varchar(256) NULL,
        subTotal varchar(256) NULL, 
        taxAmount varchar(256) NULL, 
        freight varchar(256) NULL, 
        totalDue varchar(256) NULL,
        modifiedDate varchar(256) NULL);
TRUNCATE TABLE dw.sales_orders
ALTER TABLE dw.sales_orders ADD CONSTRAINT PK_dw_sales_orders PRIMARY KEY (salesOrderId);

CREATE TABLE IF NOT EXISTS dw.customers(
        customerId varchar(256) NULL, 
        personId varchar(256) NULL, 
        storeId varchar(256) NULL, 
        territory varchar(256) NULL, 
        accountNumber varchar(256) NULL, 
        modifiedDate varchar(256) NULL);
TRUNCATE TABLE dw.customers
ALTER TABLE dw.customers ADD CONSTRAINT PK_dw_customers PRIMARY KEY (customerId);

CREATE TABLE IF NOT EXISTS dw.credit_cards(
        creditCardId varchar(256) NULL, 
        cardType varchar(256) NULL, 
        cardNumber varchar(256) NULL, 
        expiryMonth varchar(256) NULL, 
        expiryYear varchar(256) NULL, 
        modifiedDate varchar(256) NULL);
TRUNCATE TABLE dw.credit_cards
ALTER TABLE dw.credit_cards ADD CONSTRAINT PK_dw_credit_cards PRIMARY KEY (creditCardId);

CREATE TABLE IF NOT EXISTS dw.currency_rates(
        currencyRateId varchar(256) NULL, 
        currencyRateDate varchar(256) NULL, 
        fromCurrencyCode varchar(256) NULL, 
        toCurrencyCode varchar(256) NULL, 
        averageRate varchar(256) NULL, 
        endOfDayRate varchar(256) NULL, 
        modifiedDate varchar(256) NULL);
TRUNCATE TABLE dw.currency_rates
ALTER TABLE dw.currency_rates ADD CONSTRAINT PK_dw_currency_rates PRIMARY KEY (currencyRateId);

CREATE TABLE IF NOT EXISTS dw.sales_persons(
        salesPersonId varchar(256) NULL, 
        territory varchar(256) NULL, 
        salesQuota varchar(256) NULL, 
        bonus varchar(256) NULL, 
        percentCommision varchar(256) NULL, 
        salesYtd varchar(256) NULL, 
        salesLastYear varchar(256) NULL, 
        modifiedDate varchar(256) NULL);
TRUNCATE TABLE dw.sales_persons
ALTER TABLE dw.sales_persons ADD CONSTRAINT PK_dw_sales_persons PRIMARY KEY (salesPersonId);

ALTER TABLE dw.sales_orders ADD CONSTRAINT FK_sales_orders_customers FOREIGN KEY (customerId) REFERENCES dw.customer (customerId)
ALTER TABLE dw.sales_orders ADD CONSTRAINT FK_sales_orders_credit_cards FOREIGN KEY (creditCardId) REFERENCES dw.credit_cards (creditCardId)
ALTER TABLE dw.sales_orders ADD CONSTRAINT FK_sales_orders_currency_rates FOREIGN KEY (currencyRateId) REFERENCES dw.currency_rates (currencyRateId)
ALTER TABLE dw.sales_orders ADD CONSTRAINT FK_sales_orders_sales_persons FOREIGN KEY (salesPersonId) REFERENCES dw.sales_persons (salesPersonId)
"""
