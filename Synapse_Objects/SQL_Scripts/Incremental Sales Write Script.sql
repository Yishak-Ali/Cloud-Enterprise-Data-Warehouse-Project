-- Stored procedure for writing incremental fact sales data, factoring in SCDs
CREATE OR ALTER PROCEDURE STG.uspFactSalesLoad @ProcessDate DATE
WITH ENCRYPTION AS
BEGIN

IF OBJECT_ID('STG.FactSales') IS NOT NULL
    DROP EXTERNAL TABLE STG.FactSales

DECLARE @Location VARCHAR(100) = CONCAT('Conformed/Facts/Fact_Sales/Incremental/',
                                        FORMAT(@ProcessDate, 'yyyy-MM-dd'))

DECLARE @CreateExternalTable NVARCHAR(2000) =
'CREATE EXTERNAL TABLE STG.FactSales
    WITH (
        LOCATION = ''' + @Location + ''',
        DATA_SOURCE = SourceDataLake,
        FILE_FORMAT = ParquetFormat
    )
    AS
    SELECT CAST(FORMAT(SO.OrderDate, ''yyyyMMdd'') AS INT) AS OrderDateKey,
           C.CustomerKey,
           DSI.StockItemKey,
           SU.SupplierKey,
           SOL.OrderLineID,
           SO.OrderID,
           SOL.Quantity,
           SOL.UnitPrice
    FROM LDW.vwSalesOrderLines SOL
    JOIN LDW.vwSalesOrders SO
        ON SOL.OrderID = SO.OrderID
    LEFT JOIN LDW.vwDimCustomersSCD C
        ON SO.CustomerID = C.CustomerID
        AND SO.OrderDate BETWEEN C.ValidFromDate AND C.ValidToDate
    LEFT JOIN LDW.vwDimStockItems DSI
        ON SOL.StockItemID = DSI.StockItemID
    LEFT JOIN LDW.vwStockItems SI
        ON DSI.StockItemID = SI.StockItemID
    LEFT JOIN LDW.vwDimSuppliers SU
        ON SI.SupplierID = SU.SupplierID
        AND SO.OrderDate BETWEEN SU.ValidFromDate AND SU.ValidToDate
    WHERE SOL.FilePathDate = ''' + CAST(@ProcessDate AS CHAR(10)) + '''
        AND SO.FilePathDate = ''' + CAST(@ProcessDate AS CHAR(10)) + ''''

EXEC sp_executesql @CreateExternalTable

END;

