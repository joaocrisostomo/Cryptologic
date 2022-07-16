CREATE DEFINER=`root`@`localhost` PROCEDURE `Get_New_Wallets`()
BEGIN
INSERT INTO Wallet (Wallet_Hash, Load_Date)
SELECT B.Wallet,
		SYSDATE()
FROM (SELECT DISTINCT Wallet
		FROM (SELECT DISTINCT Trx_From_Wallet as Wallet FROM TRANSACTIONS
		UNION SELECT DISTINCT Trx_To_Wallet as Wallet FROM TRANSACTIONS)as A) as B
WHERE B.Wallet not in (SELECT Wallet_Hash FROM WALLET)
;
END