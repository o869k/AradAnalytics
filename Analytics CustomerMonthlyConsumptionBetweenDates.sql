USE RoundRockTX
GO
CREATE PROCEDURE Analytics.CustomerMonthlyConsumptionsBetweenDates 
	@DateFrom date = NULL,
	@DateTo date = NULL,
	@ConsumerID nvarchar(15)
AS
	IF @DateTo is NULL set @DateTo = getDate();
	IF @DateFrom is NULL set @DateFrom = DateAdd(yy, -1, @DateTo);
set @DateFrom = DATEADD(day, -day(@DateFrom)+1, @DateFrom)
set @DateTo = EOMONTH(@DateTo)

SELECT YEAR(ConsInterval)as year_, MONTH(ConsInterval) as monthNo, SUM(Cons) as monthlyCons
  FROM [MetersConsDailyTable] 
  Where 
  MeterCount = 
	(SELECT MeterCount from MeterCardDetails
		where 
			ConsumerID = @ConsumerID
	)
	and
	ConsInterval Between @DateFrom and @DateTo
	and
	ConsValid = 1
  GROUP BY Year(ConsInterval), MONTH(ConsInterval)
GO