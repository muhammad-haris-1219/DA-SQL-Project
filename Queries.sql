IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'FDAFD')
CREATE DATABASE FDAFD;
use  FDAFD;

bulk insert Transactions from 'C:\Users\dell\Desktop\Test Order SQL\Transaction.csv'
with(
fieldterminator =',', rowterminator='\n', firstrow=2
);
create table Transactions(
TransactionID int,
TransactionDate datetime,
TransactionAmount decimal(10,3),
TransactionType varchar(max),
AccountType varchar(max),
[Location] varchar(max),
[Status] varchar(max),
primary key (TransactionID),
CustomerID  int foreign key references Customers(CustomerID)
on delete cascade
);

create table Customers (
CustomerID int unique not null,
CustomerName varchar(max),
DOB datetime,
AccountBalance decimal (10,3),
JoinDate date,
Email nvarchar(max),
PhoneNumber nvarchar(max)
);
bulk insert  Customers from 'C:\Users\dell\Desktop\Test Order SQL\Customers.csv'
with(
fieldterminator =',', rowterminator='\n', firstrow=2
);

--Data Exploration and Overview
--•	Query the total number of customers and the total number of transactions in the dataset.
select 
(select count (*) from  Transactions)as Total_Transactions,
(select count (*)   from  Customers)as Total_Customers ;

--•	Find the total transaction volume for all transactions (sum of all TransactionAmount values).
select sum(TransactionAmount) as Total_Transaction  from  Transactions;

--•	Identify the top 5 customers based on the highest account balance.
select distinct top 5 CustomerID, CustomerName, AccountBalance from  Customers order by AccountBalance desc;

--•	Determine the average transaction amount for each TransactionType.
select TransactionType, avg(TransactionAmount) Avg_Amount from Transactions group by TransactionType;
 
--•	Provide a list of distinct account types from the Transactions table.
select distinct AccountType from Transactions;

-- Time Series Analysis:
--•	Calculate the monthly total transaction volume for each account type over the past 2 years.
select  [Month], [Checking], [Loan], [Savings] from
(select  distinct datename(month, TransactionDate) as [Month], sum(TransactionAmount) as Total , AccountType from Transactions
 where  DATEDIFF(year, TransactionDate,getdate())<=2
group by datename(month, TransactionDate) ,AccountType ) as Transforming
pivot(
sum(total)
for AccountType in ([Checking], [Loan], [Savings])
) as pivoting ;

--•	Find the average transaction amount per month for each transaction type.
select  [Month], [Checking], [Loan], [Savings] from
(select  distinct datename(month, TransactionDate) as [Month], avg(TransactionAmount) as Total , AccountType from Transactions
group by datename(month, TransactionDate) ,AccountType ) as Transforming
pivot(
avg(total)
for AccountType in ([Checking], [Loan], [Savings])
) as pivoting ;

--•	Identify seasonal trends by analyzing the transaction amounts during different months or quarters.
with Seasonal_Trends as
(
select DATENAME(month,transactionDate) as [Month], sum(transactionAmount) as Current_Total, 
lag(sum(transactionAmount),1) over (order by DATENAME(month,transactionDate)) as Previous_Total
from  Transactions group by DATENAME(month,transactionDate)
)
select [Month], Current_Total, Previous_Total, cast (((Current_Total- Previous_Total)/ Previous_Total)*100 as varchar)+' %'  as [Growth_Rate] 
from Seasonal_Trends order by [month];

--•	Find customers who haven't made any transactions in the last 6 months.
select CustomerName, AccountBalance, Email from customers left join transactions  on 
 customers.customerID=transactions.customerID and  datediff(month,transactionDate,getdate()) <=6 where transactions.customerID IS NULL;

 --Fraud Detection and Anomaly Analysis:
 --• Identify potential duplicate transactions: 
 --transactions that occur within a short time frame (e.g., within 1 minute) for the same customer and with the same amount.
 with Duplication as
 (select CustomerName, TransactionDate,TransactionAmount,
  lag(CustomerName,1) over(order by TransactionDate) as PreviousCustomer,
 lag(TransactionDate,1) over(order by TransactionDate) as PreviousDate,
   lag(TransactionAmount,1) over(order by TransactionDate) as PreviousAmount
 from Transactions join customers on Transactions.CustomerID= Customers.CustomerID)
 select * from Duplication  where PreviousCustomer is not null and PreviousAmount is not null
 and PreviousDate is not null and DATEDIFF(minute,PreviousDate,TransactionDate)<=2;

--•	Detect suspiciously large transactions: 
--transactions where the TransactionAmount is more than the customer's average transaction amount or top 1%.
select * from 
(select CustomerID,TransactionAmount,
PERCENTILE_CONT(.99) within group (order by transactionamount) over () as Percentile99
from Transactions) as TopData
where transactionAmount>  Percentile99;

--•	Find all failed transactions and analyze their patterns (e.g., frequency of failed transactions per customer).
select [status], [Deposit],[Withdrawal],[Loan Payment],[Transfer] from
(select  transactionType, [status], count(status) as FailedStatus  from transactions  group by transactiontype,[status]) as counts
pivot(
max(FailedStatus)
for  transactionType in  ([Deposit],[Withdrawal],[Loan Payment],[Transfer] )
) as FailedTransactions where [status]='Failed' ;

--•	Identify customers with sudden large withdrawals: withdrawals where the TransactionAmount is greater than 30% of their AccountBalance.
select  TransactionAmount, AccountBalance, (AccountBalance * 0.3) as IncreasedBalance
from transactions join customers on transactions.customerID = customers.customerID 
where TransactionType='Withdrawal' and  TransactionAmount > (AccountBalance * 0.3);

-- Customer Behavior Insights:
--•	Rank customers based on their total transaction volume over the last year.
select customerName, sum(transactionAmount) as Total from transactions join customers 
on transactions.customerID = customers.customerID where DATEDIFF(year,transactionDate,getdate())<=1 group by customerName;

--•	Identify customers who have high account balances but low transaction activity 
--(e.g., fewer than 3 transactions in the past year).
with CustomerActivity as
(select customerName, accountbalance, COUNT(transactionID) as TransactionTotal  from customers left join transactions
on transactions.customerID = customers.customerID and DATEDIFF(year,transactionDate,getdate())<=1
group by customerName, accountbalance
having accountbalance> (select avg( accountbalance) from customers ) and  COUNT(transactionID) between 1 and 3)
select * from CustomerActivity ;

--•	Determine the most popular transaction types based on the frequency of transactions by customer.
select TransactionType ,count(customers.customerID ) as CustomerFrequency from transactions  join customers 
on transactions.customerID = customers.customerID group by transactiontype order by CustomerFrequency ;

--Segmentation and Clustering:
--•	Segment customers into three categories based on their average transaction size
select TransactionType,
case
when avg(transactionamount)<1000 then 'Small' 
when avg(transactionamount) between 1000 and 10000 then 'Medium'
else 'large'
end as Category
from transactions group by TransactionType;

--•	For each segment, calculate:Total number of customers, Total transaction volume, Average number of transactions per customer
with Segmentations as(
select CustomerID,
case 
when avg(TransactionAmount) <1000 then 'Small'
when avg(TransactionAmount) between 1000 and 10000  then 'Medium'
else 'Large'
end as Segments
from Transactions group by CustomerID)
select 
Segments, COUNT (CustomerID ) as Total_Customers, sum(TransactionAmount) as Total_Volume, avg(TransactionsPerCustomer) as Transac_PerCustomer
from
(select Segmentations.Segments, Transactions.CustomerID, Transactions.TransactionAmount,
COUNT(*) over(partition by Transactions.CustomerID) as TransactionsPerCustomer -- this statement used only when IDs are unique
from Transactions join Segmentations on
Segmentations.CustomerID=Transactions.CustomerID 
) as ProvidingInfo group by Segments ;

--Custom Reports:
--•	Generate a monthly report as Total transactions, volume, failed & customers making transactions 
select   year(TransactionDate) as [Year], datename(MONTH, TransactionDate) as [Month],
COUNT(TransactionID) as [Transactions],
sum(TransactionAmount) as TransactionVolume, 
count(CustomerID) as CustomersMakingTransactions,
sum(case when [status]= 'Failed' then 1 else 0 end) as FailedTransaction
from Transactions  group by datename(MONTH, TransactionDate), DATEPART(YEAR, TransactionDate) ;

--•	Create report of inactive customers who haven’t made any transactions in the last year & current account balance.
select Customers.CustomerID , CustomerName, AccountBalance from Transactions right join Customers
on Transactions.CustomerID = Customers.CustomerID  
and TransactionDate >= DATEADD(year,-1,GETDATE())
where TransactionID is null;

--•	Build query that tracks customers make more than 10 transactions per week & flag those with a sudden increase in transaction volume.
with cte as(
select year(TransactionDate) as [Year],datepart(WEEK,TransactionDate) as [Week],count(TransactionID) As TransactionsCount ,
lag(count(TransactionID),1) over(order by  year(TransactionDate),Datepart(WEEK,TransactionDate) ) as previous 
from Transactions group by  year(TransactionDate),datepart(WEEK,TransactionDate) having count(TransactionID)>=10 )
select [Year], [week], TransactionsCount, previous ,
CASE WHEN previous IS NOT NULL AND TransactionsCount > previous * 1.5 THEN 'Sudden Increase' ELSE 'No Increase'
END AS IncreaseFlag from cte WHERE (CASE 
 WHEN previous IS NOT NULL AND TransactionsCount > previous * 1.5 THEN 'Sudden Increase' ELSE 'No Increase' 
END) = 'Sudden Increase' order by [Year];


--Forecasting and Predictions:
--•	Calculate the moving average of monthly transaction volume for the last 2 years.
with MonthlyTransactionsVolume as
(select  year(TransactionDate) as [Year], DATENAME(month,TransactionDate) as [Month],
sum(TransactionAmount) as MonthlyAmount
from Transactions where DATEDIFF(year,TransactionDate,GETDATE())<=2
group by DATENAME(month,TransactionDate) ,year(TransactionDate))
select [Year], [Month],MonthlyAmount,
avg(MonthlyAmount) over(order by [year], [month] rows BETWEEN 11 PRECEDING and current row) as MovingAverage
from MonthlyTransactionsVolume order by [year];

--•	Predict the total transaction volume for the next quarter based on historical trends.
with QuarterlyVolumes as 
(select year(transactiondate) as [Year], datename(QUARTER,transactiondate) as [Quarter],
sum(transactionAmount) as TotalAmount from Transactions
group by year(transactiondate) ,datename(QUARTER,transactiondate) ),
GrowthRateCalc as
(select [Year],[Quarter], TotalAmount, lag(TotalAmount,1) over( order by [Year],[quarter]) as PreviousAmount
from QuarterlyVolumes) ,
GrowthRating as
(select [Year],[Quarter],avg((TotalAmount- PreviousAmount )/TotalAmount) as GrowthRate
from GrowthRateCalc where previousAmount is not null group by [Year],[Quarter] )
SELECT 
 CurrentQuarterAmount=(select totalAmount from QuarterlyVolumes where [year]=Year(getdate()) and [Quarter]=DATEPART(quarter,getdate())),
 PredictedQuarterAmount=(select totalAmount from QuarterlyVolumes where [year]=Year(getdate()) and [Quarter]=DATEPART(quarter,getdate()))
 *(1+
 (select  GrowthRate from GrowthRating where [year]=Year(getdate()) and [Quarter]=DATEPART(quarter,getdate()))
 );

--•Find customers who have significant increase in transaction volume compared to historical average & might require further attention or monitoring.
select Customers.CustomerID, CustomerName, sum(TransactionAmount) as TransactionVolume from Transactions join Customers 
on Transactions.CustomerID= Customers.CustomerID where TransactionAmount> 
(select AVG(TransactionAmount) as a from Transactions)*1.8
group by  Customers.CustomerID, CustomerName;

