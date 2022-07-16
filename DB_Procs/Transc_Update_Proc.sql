CREATE PROCEDURE `new_procedure` ()
BEGIN
INSERT INTO TRANSACTIONS
	SELECT A.Trx_From_Wallet,
			A.Trx_To_Wallet,
            A.Trx_Amount,
            A.Trx_Datetime,
            A.Trx_Hash,
            A.Trx_Gas,
            A.Trx_GasPrice,
            A.Trx_Status,
            A.Trx_Method_ID
	FROM (SELECT B.*,
				A.Trx_Hash as TRXHASH
		FROM TRANSACTIONS_AUX as B
		LEFT JOIN TRANSACTIONS as A
		ON B.Trx_Hash = A.Trx_Hash) as A
	WHERE A.TRXHASH is Null;
END
