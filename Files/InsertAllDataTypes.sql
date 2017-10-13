DECLARE @minbigint BIGINT = -9223372036854775808
DECLARE @maxbigint BIGINT = 9223372036854775807

DECLARE @minint INT = -2147483648
DECLARE @maxint INT = 2147483647

DECLARE @minsmallint SMALLINT = -32768
DECLARE @maxsmallint SMALLINT = 32767

DECLARE @mintinyint TINYINT = 0
DECLARE @maxtinyint TINYINT = 255

DECLARE @minmoney MONEY = -922337203685477.5808
DECLARE @maxmoney MONEY = 922337203685477.5807

DECLARE @minsmallmoney SMALLMONEY = -214748.3648
DECLARE @maxsmallmoney SMALLMONEY = 214748.3647

DECLARE @minnumeric NUMERIC(10, 4) = -922337.8547
DECLARE @maxnumeric NUMERIC(10, 4) = 922337.8547

DECLARE @mindecimal DECIMAL(10, 4) = -922337.8547
DECLARE @maxdecimal DECIMAL(10, 4) = 922337.8547

DECLARE @minfloat FLOAT(20) = -922337203685477.5808
DECLARE @maxfloat FLOAT(20) = 922337203685477.5807

DECLARE @minreal REAL = -922337203685477.5808
DECLARE @maxreal REAL = 922337203685477.5807

DECLARE @mindate DATE = '1900-01-01'
DECLARE @maxdate DATE = '2100-01-01'

DECLARE @mindatetime DATETIME = '1900-01-01 13:45'
DECLARE @maxdatetime DATETIME = '2100-01-01 16:45'

DECLARE @mindatetime2 DATETIME = '1900-01-01 13:45'
DECLARE @maxdatetime2 DATETIME = '2100-01-01 16:45'

DECLARE @minsmalldatetime SMALLDATETIME = '1900-01-01'
DECLARE @maxsmalldatetime SMALLDATETIME = '2079-06-06'

DECLARE @mintime TIME = '00:00:00.0000000'
DECLARE @maxtime TIME = '23:59:59.9999999'

DECLARE @mindatetimeoffset DATETIMEOFFSET = '0001-01-01'
DECLARE @maxdatetimeoffset DATETIMEOFFSET = '9999-12-31'

DECLARE @minvarchar VARCHAR(100) = 'qwertzuiop1234567890'
DECLARE @maxvarchar VARCHAR(100) = 'qwertzuiop1234567890asdfghjkl1234567890yxcvbnm1234567890'

DECLARE @minnvarchar NVARCHAR(100) = N'üöäqwertzuiop1234567890'
DECLARE @maxnvarchar NVARCHAR(100) = N'üöäqwertzuiop1234567890asdfghjkl1234567890yxcvbnm1234567890'

DECLARE @minchar CHAR(10) = N'qwertzuiop'
DECLARE @maxchar CHAR(10) = N'qwertzuiop'

DECLARE @minnchar NCHAR(10) = N'üöäqwertzu'
DECLARE @maxnchar NCHAR(10) = N'üöäqwertzu'

DECLARE @mintext VARCHAR(MAX) = N'Some text.'
DECLARE @maxtext VARCHAR(MAX) = N'More text without meaning.'

DECLARE @minntext NVARCHAR(MAX) = N'Some text.'
DECLARE @maxntext NVARCHAR(MAX) = N'More text without meaning.'

DECLARE @minbinary BINARY(10) = CAST(12345678901234567890 AS BINARY(10))
DECLARE @maxbinary BINARY(10) = CAST(12345678901234567890123456789012345678 AS BINARY(10))

DECLARE @minvarbinary VARBINARY(100) = CAST(12345678901234567890 AS VARBINARY(100))
DECLARE @maxvarbinary VARBINARY(100) = CAST(0x1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890 AS VARBINARY(100))

DECLARE @minimage VARBINARY(MAX) = CAST(12345678901234567890 AS VARBINARY(MAX))
DECLARE @maximage VARBINARY(MAX) = CAST(0x1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890 AS VARBINARY(MAX))

DECLARE @minbit BIT = 0
DECLARE @maxbit BIT = 1

INSERT INTO #TargetTable 
		(BigintCol, IntCol, SmallintCol, TinyintCol
		, MoneyColumn, SmallmoneyColumn
		, NumericColumn, DecimalColumn, FloatColumn, RealColumn
		, DateColumn, DatetimeColumn, Datetime2Column, SmalldatetimeColumn, TimeColumn, DatetimeoffsetColumn
		, VarcharColumn, NvarcharColumn, CharColumn, NcharColumn, TextColumn, NtextColumn
		, BinaryColumn, VarbinaryColumn, ImageColumn
		, BitColumn)
VALUES (@minbigint, @minint, @minsmallint, @mintinyint
		, @minmoney, @minsmallmoney
		, @minnumeric, @mindecimal, @minfloat, @minreal
		, @mindate, @mindatetime, @mindatetime2, @minsmalldatetime, @mintime, @mindatetimeoffset
		, @minvarchar, @minnvarchar, @minchar, @minnchar, @mintext, @minntext
		, @minbinary, @minvarbinary, @minimage
		, @minbit);
		
INSERT INTO #TargetTable  
		(BigintCol, IntCol, SmallintCol, TinyintCol
		, MoneyColumn, SmallmoneyColumn
		, NumericColumn, DecimalColumn, FloatColumn, RealColumn
		, DateColumn, DatetimeColumn, Datetime2Column, SmalldatetimeColumn, TimeColumn, DatetimeoffsetColumn
		, VarcharColumn, NvarcharColumn, CharColumn, NcharColumn, TextColumn, NtextColumn
		, BinaryColumn, VarbinaryColumn, ImageColumn
		, BitColumn)
VALUES (@maxbigint, @maxint, @maxsmallint, @maxtinyint
		, @maxmoney, @maxsmallmoney
		, @maxnumeric, @maxdecimal, @maxfloat, @maxreal
		, @maxdate, @maxdatetime, @maxdatetime2, @maxsmalldatetime, @maxtime, @maxdatetimeoffset
		, @maxvarchar, @maxnvarchar, @maxchar, @maxnchar, @maxtext, @maxntext
		, @maxbinary, @maxvarbinary, @maximage
		, @maxbit);