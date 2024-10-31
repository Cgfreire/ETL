import datetime
import requests,json
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

STG_SALES_ORDERS_CREATION = """CREATE TABLE IF NOT EXISTS stg.sales_orders(
        salesOrderId varchar(256) NULL,  orderDate varchar(256) NULL, dueDate varchar(256) NULL, shipDate varchar(256) NULL,status varchar(256) NULL, isOrderedOnline varchar(256) NULL, salesOrderNumber varchar(256) NULL, purchaseOrderNumber varchar(256) NULL,
        accountNumber varchar(256) NULL, customerId varchar(256) NULL, salesPersonId varchar(256) NULL, territory varchar(256) NULL,billToAddress varchar(256) NULL, shipToAddress varchar(256) NULL, shipMethod varchar(256) NULL, creditCardId varchar(256) NULL,
        creditCardApprovalCode varchar(256) NULL, currencyRateId varchar(256), subTotal varchar(256) NULL, taxAmount varchar(256) NULL, freight varchar(256) NULL, totalDue varchar(256) NULL,modifiedDate varchar(256) NULL);
        TRUNCATE TABLE stg.sales_orders"""

STG_CUSTOMERS_CREATION = """CREATE TABLE IF NOT EXISTS stg.customers(
        customerId varchar(256) NULL, personId varchar(256) NULL, storeId varchar(256) NULL, territory varchar(256) NULL, accountNumber varchar(256) NULL, modifiedDate varchar(256) NULL);
        TRUNCATE TABLE stg.customers"""
   
STG_CREDIT_CARDS_CREATION = """CREATE TABLE IF NOT EXISTS stg.credit_cards(
        creditCardId varchar(256) NULL, cardType varchar(256) NULL, cardNumber varchar(256) NULL, expiryMonth varchar(256) NULL, expiryYear varchar(256) NULL, modifiedDate varchar(256) NULL);
        TRUNCATE TABLE stg.credit_cards"""

STG_CURRENCY_RATES_CREATION = """CREATE TABLE IF NOT EXISTS stg.currency_rates(
        currencyRateId varchar(256) NULL, currencyRateDate varchar(256) NULL, fromCurrencyCode varchar(256) NULL, toCurrencyCode varchar(256) NULL, averageRate varchar(256) NULL, endOfDayRate varchar(256) NULL, modifiedDate varchar(256) NULL);
        TRUNCATE TABLE stg.currency_rates"""
        
STG_SALES_PERSONS_CREATION = """CREATE TABLE IF NOT EXISTS stg.sales_persons(
        salesPersonId varchar(256) NULL, territory varchar(256) NULL, salesQuota varchar(256) NULL, bonus varchar(256) NULL, percentCommision varchar(256) NULL, salesYtd varchar(256) NULL, salesLastYear varchar(256) NULL, modifiedDate varchar(256) NULL);
        TRUNCATE TABLE stg.sales_persons"""
             
def INSERT_STG_SALES_ORDERS(salesOrderId, orderDate,dueDate,shipDate,status,isOrderedOnline,salesOrderNumber,purchaseOrderNumber,accountNumber,customerId,salesPersonId,territory,billToAddress,shipToAddress,shipMethod,creditCardId,creditCardApprovalCode, currencyRateId, subTotal,taxAmount,freight,totalDue,modifiedDate):
    return f"""INSERT INTO stg.sales_orders (salesOrderId, orderDate,dueDate,shipDate,status,isOrderedOnline,salesOrderNumber,purchaseOrderNumber,accountNumber,customerId,salesPersonId,territory,billToAddress,shipToAddress,shipMethod,creditCardId, creditCardApprovalCode,currencyRateId,subTotal,taxAmount,freight,totalDue,modifiedDate) 
               VALUES ('{salesOrderId}', '{orderDate}','{dueDate}','{shipDate}','{status}','{isOrderedOnline}','{salesOrderNumber}','{purchaseOrderNumber}','{accountNumber}','{customerId}','{salesPersonId}','{territory}','{billToAddress}','{shipToAddress}','{shipMethod}','{creditCardId}','{creditCardApprovalCode}','{currencyRateId}','{subTotal}','{taxAmount}','{freight}','{totalDue}','{modifiedDate}') """

def INSERT_STG_CUSTOMERS(customerId, personId, storeId,territory, accountNumber, modifiedDate):
    return f"""INSERT INTO stg.customers (customerId, personId, storeId, territory, accountNumber, modifiedDate) 
               VALUES ('{customerId}', '{personId}', '{storeId}', '{territory}', '{accountNumber}', '{modifiedDate}') """

def INSERT_STG_CREDIT_CARDS(creditCardId, cardType, cardNumber,expiryMonth, expiryYear, modifiedDate):
    return f"""INSERT INTO stg.credit_cards (creditCardId, cardType, cardNumber,expiryMonth, expiryYear, modifiedDate) 
               VALUES ('{creditCardId}', '{cardType}', '{cardNumber}', '{expiryMonth}', '{expiryYear}', '{modifiedDate}') """

def INSERT_STG_CURRENCY_RATES(currencyRateId, currencyRateDate, fromCurrencyCode,toCurrencyCode, averageRate, endOfDayRate, modifiedDate):
    return f"""INSERT INTO stg.currency_rates (currencyRateId, currencyRateDate, fromCurrencyCode,toCurrencyCode, averageRate, endOfDayRate, modifiedDate) 
               VALUES ('{currencyRateId}', '{currencyRateDate}', '{fromCurrencyCode}', '{toCurrencyCode}', '{averageRate}', '{endOfDayRate}', '{modifiedDate}') """

def INSERT_STG_SALES_PERSONS(salesPersonId, territory, salesQuota, bonus, percentCommision, salesYtd, salesLastYear, modifiedDate):
    return f"""INSERT INTO stg.sales_persons (salesPersonId, territory, salesQuota, bonus, percentCommision, salesYtd, salesLastYear, modifiedDate) 
               VALUES ('{salesPersonId}', '{territory}', '{salesQuota}', '{bonus}', '{percentCommision}', '{salesYtd}', '{salesLastYear}', '{modifiedDate}') """
    
                     
def coleta_dados_stg():
    pg_hook = PostgresHook('postgrees-airflow')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(STG_SALES_ORDERS_CREATION)
    cursor.execute(STG_CUSTOMERS_CREATION)
    cursor.execute(STG_CREDIT_CARDS_CREATION)
    cursor.execute(STG_CURRENCY_RATES_CREATION)
    cursor.execute(STG_SALES_PERSONS_CREATION)
    pageNumber = 1
    for i in range(pageNumber, 100):
        response = requests.get(url=f'https://demodata.grapecity.com/adventureworks/api/v1/salesOrders?PageNumber={pageNumber}&PageSize=500')
        for j in json.loads(response.text):
            salesOrderId =j['salesOrderId']
            orderDate =j['orderDate']
            dueDate =j['dueDate']
            shipDate =j['shipDate']
            status =j['status']
            isOrderedOnline =j['isOrderedOnline']
            salesOrderNumber =j['salesOrderNumber']
            purchaseOrderNumber =j['purchaseOrderNumber']
            accountNumber =j['accountNumber']
            customerId =j['customerId']
            salesPersonId =j['salesPersonId']
            territory =j['territory']
            billToAddress =j['billToAddress']
            shipToAddress =j['shipToAddress']
            shipMethod =j['shipMethod']
            creditCardId =j['creditCardId']
            creditCardApprovalCode = j['creditCardApprovalCode']
            currencyRateId = j['currencyRateId']
            subTotal =j['subTotal']
            taxAmount =j['taxAmount']
            freight =j['freight']
            totalDue =j['totalDue']
            modifiedDate=j['modifiedDate']
            cursor.execute(INSERT_STG_SALES_ORDERS(salesOrderId, orderDate,dueDate,shipDate,status,isOrderedOnline,salesOrderNumber,purchaseOrderNumber,accountNumber,
                                            customerId,salesPersonId,territory,billToAddress,shipToAddress,shipMethod,creditCardId,creditCardApprovalCode, currencyRateId,subTotal,taxAmount,freight,
                                            totalDue,modifiedDate))
            
    for i in range(pageNumber, 100):
        response = requests.get(url=f'https://demodata.grapecity.com/adventureworks/api/v1/customers?PageNumber={pageNumber}&PageSize=500')
        for j in json.loads(response.text):
            customerId = j['customerId']
            personId = j['personId']
            storeId = j['storeId']
            territory = j['territory']
            accountNumber = j['accountNumber']
            modifiedDate = j['modifiedDate']
            cursor.execute(INSERT_STG_CUSTOMERS(customerId, personId, storeId,territory, accountNumber, modifiedDate))
            
    for i in range(pageNumber, 100):
        response = requests.get(url=f'https://demodata.grapecity.com/adventureworks/api/v1/creditCards?PageNumber={pageNumber}&PageSize=500')
        for j in json.loads(response.text):
            creditCardId = j['creditCardId']
            cardType = j['cardType']
            cardNumber = j['cardNumber']
            expiryMonth = j['expiryMonth']
            expiryYear = j['expiryYear'] 
            modifiedDate = j['modifiedDate']
            cursor.execute(INSERT_STG_CREDIT_CARDS(creditCardId, cardType, cardNumber,expiryMonth, expiryYear, modifiedDate))
            
    for i in range(pageNumber, 100):
        response = requests.get(url=f'https://demodata.grapecity.com/adventureworks/api/v1/currencyRates?PageNumber={pageNumber}&PageSize=500')
        for j in json.loads(response.text):
            currencyRateId = j['currencyRateId'] 
            currencyRateDate = j['currencyRateDate'] 
            fromCurrencyCode = j['fromCurrencyCode']
            toCurrencyCode = j['toCurrencyCode']
            averageRate = j['averageRate']
            endOfDayRate = j['endOfDayRate']
            modifiedDate = j['modifiedDate']
            cursor.execute(INSERT_STG_CURRENCY_RATES(currencyRateId, currencyRateDate, fromCurrencyCode,toCurrencyCode, averageRate, endOfDayRate, modifiedDate))
    
    for i in range(pageNumber, 100):
        response = requests.get(url='https://demodata.grapecity.com/adventureworks/api/v1/salesPersons')
        for j in json.loads(response.text):
            salesPersonId = j['salesPersonId'] 
            territory = j['territory']
            salesQuota = j['salesQuota']
            bonus = j['bonus']
            percentCommision = j['percentCommision']
            salesYtd = j['salesYtd']
            salesLastYear = j['salesLastYear']
            modifiedDate = j['modifiedDate']
            cursor.execute(INSERT_STG_SALES_PERSONS(salesPersonId, territory, salesQuota, bonus, percentCommision, salesYtd, salesLastYear, modifiedDate))
                 
    conn.commit()   
    cursor.close()
    conn.close()      
                          
with DAG(dag_id ='coleta_dados_stg',start_date=datetime.datetime(2024, 9, 2), schedule="@daily") as dag:
    coleta_dados_stg = PythonOperator(task_id = 'coleta_dados_stg', python_callable = coleta_dados_stg)
    cria_dw = PostgresOperator(task_id = 'cria_dw', postgres_conn_id = 'postgrees-airflow', 
                                    sql ="""DROP TABLE IF EXISTS dw.sales_orders;
                                CREATE TABLE IF NOT EXISTS dw.sales_orders(
        IdDwSalesOrders INTEGER GENERATED ALWAYS AS IDENTITY,
        salesOrderId INTEGER,  
        orderDate DATE, 
        dueDate DATE, 
        shipDate DATE,
        status varchar(256) NULL, 
        isOrderedOnline BIT, 
        salesOrderNumber varchar(256) NULL, 
        purchaseOrderNumber varchar(256) NULL,
        accountNumber varchar(256) NULL, 
        customerId INTEGER, 
        salesPersonId INTEGER, 
        territory varchar(256) NULL,
        billToAddress varchar(256) NULL, 
        shipToAddress varchar(256) NULL, 
        shipMethod varchar(256) NULL, 
        creditCardId INTEGER,
        creditCardApprovalCode varchar(256) NULL, 
        currencyRateId INTEGER,
        subTotal NUMERIC(15,2), 
        taxAmount NUMERIC(15,2), 
        freight NUMERIC(15,2), 
        totalDue NUMERIC(15,2),
        modifiedDate DATE);
TRUNCATE TABLE dw.sales_orders;
ALTER TABLE dw.sales_orders ADD CONSTRAINT PK_dw_sales_orders PRIMARY KEY (salesOrderId);

DROP TABLE IF EXISTS dw.customers;
CREATE TABLE IF NOT EXISTS dw.customers(
        IdDwcustomer INTEGER GENERATED ALWAYS AS IDENTITY,
        customerId INTEGER, 
        personId varchar(256) NULL, 
        storeId INTEGER, 
        territory varchar(256) NULL, 
        accountNumber varchar(256) NULL, 
        modifiedDate DATE NULL);
TRUNCATE TABLE dw.customers;
ALTER TABLE dw.customers ADD CONSTRAINT PK_dw_customers PRIMARY KEY (customerId);

DROP TABLE IF EXISTS dw.credit_cards;
CREATE TABLE IF NOT EXISTS dw.credit_cards(
        IdDwCreditCard INTEGER GENERATED ALWAYS AS IDENTITY,
        creditCardId INTEGER, 
        cardType varchar(256) NULL, 
        cardNumber varchar(256) NULL, 
        expiryMonth INTEGER, 
        expiryYear INTEGER, 
        modifiedDate DATE);
TRUNCATE TABLE dw.credit_cards;
ALTER TABLE dw.credit_cards ADD CONSTRAINT PK_dw_credit_cards PRIMARY KEY (creditCardId);

DROP TABLE IF EXISTS dw.currency_rates;
CREATE TABLE IF NOT EXISTS dw.currency_rates(
        IdDwCurrencyRate INTEGER GENERATED ALWAYS AS IDENTITY,
        currencyRateId INTEGER, 
        currencyRateDate DATE, 
        fromCurrencyCode varchar(256) NULL, 
        toCurrencyCode varchar(256) NULL, 
        averageRate NUMERIC(15,2) NULL, 
        endOfDayRate NUMERIC(15,2) NULL, 
        modifiedDate DATE NULL);
TRUNCATE TABLE dw.currency_rates;
ALTER TABLE dw.currency_rates ADD CONSTRAINT PK_dw_currency_rates PRIMARY KEY (currencyRateId);

DROP TABLE IF EXISTS dw.sales_persons;
CREATE TABLE IF NOT EXISTS dw.sales_persons(
        IdDwSalesPerson INTEGER GENERATED ALWAYS AS IDENTITY,
        salesPersonId INTEGER NULL, 
        territory varchar(256) NULL, 
        salesQuota varchar(256) NULL, 
        bonus NUMERIC(15,2) NULL, 
        percentCommision NUMERIC(15,2) NULL, 
        salesYtd NUMERIC(15,2) NULL, 
        salesLastYear NUMERIC(15,2) NULL, 
        modifiedDate DATE NULL);
TRUNCATE TABLE dw.sales_persons;
ALTER TABLE dw.sales_persons ADD CONSTRAINT PK_dw_sales_persons PRIMARY KEY (salesPersonId);

ALTER TABLE dw.sales_orders ADD CONSTRAINT FK_sales_orders_customers FOREIGN KEY (customerId) REFERENCES dw.customers (customerId);
ALTER TABLE dw.sales_orders ADD CONSTRAINT FK_sales_orders_credit_cards FOREIGN KEY (creditCardId) REFERENCES dw.credit_cards (creditCardId);
ALTER TABLE dw.sales_orders ADD CONSTRAINT FK_sales_orders_currency_rates FOREIGN KEY (currencyRateId) REFERENCES dw.currency_rates (currencyRateId);
--ALTER TABLE dw.sales_orders ADD CONSTRAINT FK_sales_orders_sales_persons FOREIGN KEY (salesPersonId) REFERENCES dw.sales_persons (salesPersonId);
""")
    
    insere_dw =  PostgresOperator(task_id = 'insere_dados_dw', postgres_conn_id = 'postgrees-airflow', 
                                    sql =  """
INSERT INTO dw.credit_cards (creditCardId, cardType, cardNumber,expiryMonth, expiryYear, modifiedDate) 
SELECT DISTINCT
		CAST(creditcardid as INTEGER), 
		cardtype, 
		cardnumber, 
		CAST(expirymonth as INTEGER), 
		CAST(expiryyear as INTEGER), 
		CAST(modifieddate as DATE)
FROM stg.credit_cards;

INSERT INTO dw.currency_rates (currencyRateId, currencyRateDate, fromCurrencyCode,toCurrencyCode, averageRate, endOfDayRate, modifiedDate) 
SELECT DISTINCT
		CAST(currencyrateid as INTEGER), 
		CAST(currencyratedate as DATE), 
		fromcurrencycode, 
		tocurrencycode, 
		CAST(averagerate as NUMERIC(15,2)), 
		CAST(endofdayrate as NUMERIC(15,2)),
		CAST(modifieddate as DATE)
FROM stg.currency_rates;

INSERT INTO dw.sales_persons (salesPersonId, territory, salesQuota, bonus, percentCommision, salesYtd, salesLastYear, modifiedDate) 
SELECT DISTINCT
		CAST(salespersonid as INTEGER), 
		territory, 
		salesquota, 
		CAST(bonus as NUMERIC(15,2)), 
		CAST(percentcommision as NUMERIC(15,2)), 
		CAST(salesytd as NUMERIC(15,2)),
		CAST(saleslastyear as NUMERIC(15,2)),
		CAST(modifieddate as DATE)
FROM stg.sales_persons;

INSERT INTO dw.customers (customerId, personId, storeId, territory, accountNumber, modifiedDate) 
SELECT DISTINCT
		CAST(customerid as INTEGER), 
		personId, 
		CAST(storeId as INTEGER), 
		CAST(territory as VARCHAR(256)), 
		accountnumber, 
		cast(modifieddate as DATE) 
FROM stg.customers; 

INSERT INTO dw.sales_orders (salesOrderId, orderDate,dueDate,shipDate,status,isOrderedOnline,salesOrderNumber,purchaseOrderNumber,accountNumber,customerId,salesPersonId,territory,billToAddress,shipToAddress,shipMethod,creditCardId, creditCardApprovalCode,currencyRateId,subTotal,taxAmount,freight,totalDue,modifiedDate) 
SELECT DISTINCT 
       NULLIF(a.salesorderid, 'None')::INTEGER,
       a.orderdate::DATE,
       a.duedate::DATE,
       a.shipdate::DATE,
       a.status,
       CASE WHEN a.isorderedonline = 'False' THEN 1::BIT ELSE 0::BIT END AS isorderedonline,
       a.salesordernumber,
       a.purchaseordernumber,
       a.accountnumber,
       e.IdDwcustomer,
       d.IdDwSalesPerson,
       a.territory,
       a.billtoaddress,
       a.shiptoaddress,
       a.shipmethod,
       b.IdDwCreditCard,
       a.creditCardApprovalCode,
       c.IdDwCurrencyRate,
       a.subtotal::NUMERIC(15,2),
       a.taxamount::NUMERIC(15,2),
       a.freight::NUMERIC(15,2),
       a.totaldue::NUMERIC(15,2),
       a.modifieddate::DATE
FROM stg.sales_orders a
LEFT JOIN dw.credit_cards b ON NULLIF(a.creditcardid, 'None')::INTEGER = b.creditcardid
LEFT JOIN dw.currency_rates c ON NULLIF(a.currencyRateId, 'None')::INTEGER = c.currencyRateId
LEFT JOIN dw.sales_persons d ON NULLIF(a.salespersonid, 'None')::INTEGER = d.salespersonid
LEFT JOIN dw.customers e ON NULLIF(a.customerid, 'None')::INTEGER = e.customerid;
"""  )                     

    coleta_dados_stg >> cria_dw >> insere_dw
    
    

    
