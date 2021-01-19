Declare @json_table nvarchar(max)
select @json_table =BulkColumn
from OPENROWSET(BULK '/var/opt/mssql/DataFile/Order_returns.json',SINGLE_CLOB) as j


  DROP TABLE IF EXISTS dbo.JSONDocuments_CustomerReturns;
  CREATE TABLE dbo.JSONDocuments_CustomerReturns
    (
    Document_id INT NOT NULL,
    returnReference VARCHAR(30) NOT NULL,
   	orderReference VARCHAR(30) NOT NULL,--holds a JSON object
  	customerId VARCHAR(30) NOT NULL,--holds an array of JSON objects
  	items NVARCHAR(MAX) NULL,--holds an array of JSON objects
  	[timestamp] VARCHAR(30) NULL

    CONSTRAINT JSONDocumentsPk PRIMARY KEY (Document_id)
    ) ON [PRIMARY];

     INSERT INTO dbo.JSONDocuments_CustomerReturns ( Document_id,returnReference,orderReference,customerId, items, [timestamp])
   SELECT [key] AS Document_id,returnReference,orderReference,customerId, items, [timestamp]
    FROM OpenJson(@json_table,'$.Orders') AS EachDocument
        CROSS APPLY OpenJson(EachDocument.Value) 
  	  WITH (
  	      returnReference VARCHAR(30) '$.returnReference', 
  		  orderReference VARCHAR(30) '$.orderReference' ,
          customerId VARCHAR(30) '$.customerId' ,
  		  items NVARCHAR(MAX) '$.items' AS JSON,
          [timestamp] VARCHAR(30) '$.timestamp'
      
  	       ) 





  --stock the table variable with the address information
  SELECT returnReference, orderReference,customerId,[timestamp],ItemsinReturns.variantId,ItemsinReturns.quantity,
  ReasonCodes.Code,ReasonCodes.notes,l.value
        
      FROM dbo.JSONDocuments_CustomerReturns
        CROSS APPLY
      OpenJson(JSONDocuments_CustomerReturns.items) AllItems
  	  CROSS APPLY 
  	   OpenJson(AllItems.value)
      WITH
        (
        variantId VARCHAR(10) '$.variantId', quantity VARCHAR(10) '$.quantity'
       ,returnReason NVARCHAR(MAX) AS json,carrierCodes nvarchar(max)
        ) ItemsinReturns
      CROSS APPLY
  	OpenJson(ItemsinReturns.returnReason) WITH
        (
        code varchar(10) '$.code',notes varchar(200) '$.notes'
        )ReasonCodes
    CROSS APPLY 
    OPENJSON (AllItems.value,'$.carrierCodes') as l



       





