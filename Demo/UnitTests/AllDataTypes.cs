using System;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SSIS.Test.Metadata;

namespace SSIS.Test.Template.Demo.UnitTests
{
    [UnitTest("DEMO", "DataTypes.dtsx", ExecutionOrderFactor = 1.9f)]
    [FakeSource(@"\[DataTypes]\[Data Flow Task]\[OLEDB All data source]",
        new[] { "OLE DB Source Output" },
        new[]
        {
            "-9223372036854775808;-2147483648;-32768;0;-922337203685477.5808;-214748.3648;-922337.8547;-922337.8547;-9.223372E+14;-9.223372E+14;1900-01-01;1900-01-01 13:45:00.000;1900-01-01 13:45:00.0000000;1900-01-01 00:00:00;00:00:00.0000000;0001-01-01 00:00:00 +00:00;qwertzuiop1234567890;üöäqwertzuiop1234567890;qwertzuiop;üöäqwertzu;Some text.;Some text.;FAAAAdIKH+uMqQ==;FAAAAdIKH+uMqVSr;FAAAAdIKH+uMqVSr;False\r\n9223372036854775807;2147483647;32767;255;922337203685477.5807;214748.3647;922337.8547;922337.8547;9.223372E+14;9.223372E+14;2100-01-01;2100-01-01 16:45:00.000;2100-01-01 16:45:00.0000000;2079-06-06 00:00:00;23:59:59.9999999;9999-12-31 00:00:00 +00:00;qwertzuiop1234567890asdfghjkl1234567890yxcvbnm1234567890;üöäqwertzuiop1234567890asdfghjkl1234567890yxcvbnm1234567890;qwertzuiop;üöäqwertzu;More text without meaning.;More text without meaning.;JgAAAU7zON5QkA==;EjRWeJASNFZ4kBI0VniQEjRWeJASNFZ4kBI0VniQEjRWeJASNFZ4kBI0VniQEjRWeJASNFZ4kBI0VniQEjRWeJASNFZ4kBI0VniQEjRWeJA=;EjRWeJASNFZ4kBI0VniQEjRWeJASNFZ4kBI0VniQEjRWeJASNFZ4kBI0VniQEjRWeJASNFZ4kBI0VniQEjRWeJASNFZ4kBI0VniQEjRWeJA=;True"
        },
        RowDelimiter = "\r\n", ColumnDelimiter = ";")]
    public class AllDataTypes1 : BaseUnitTest
    {
        protected override void Setup(SetupContext context)
        {
            context.Package.GetConnection("CustomerDB").SetConnectionString(Constants.SsisDbConnectionString);

            string sql = AllDataTypesUtil.GetInsertSql("[dbo].[AllDataTypesSource]");
            context.DataAccess.OpenConnection(Constants.DbConnectionString);
            context.DataAccess.ExecuteNonQuery("DELETE FROM [dbo].[AllDataTypesSource]");
            context.DataAccess.ExecuteNonQuery("DELETE FROM [dbo].[AllDataTypesTarget]");
            context.DataAccess.ExecuteNonQuery(sql);
            context.DataAccess.CloseConnection();
        }

        protected override void Verify(VerificationContext context)
        {
            // Source              
            context.DataAccess.OpenConnection(Constants.DbConnectionString);

            const string sqlSource = "SELECT * FROM [dbo].[AllDataTypesSource]";

            SqlDataReader readerSource = context.DataAccess.ExecuteReader(sqlSource);

            if (!readerSource.Read())
                throw new InvalidOperationException("Empty source reader.");
            var sourceValues1 = new object[readerSource.FieldCount];
            readerSource.GetValues(sourceValues1);

            if (!readerSource.Read())
                throw new InvalidOperationException("Empty source reader.");
            var sourceValues2 = new object[readerSource.FieldCount];
            readerSource.GetValues(sourceValues2);

            context.DataAccess.CloseConnection();

            // Target
            context.DataAccess.OpenConnection(Constants.DbConnectionString);

            const string sqlTarget = "SELECT * FROM [dbo].[AllDataTypesTarget]";

            SqlDataReader readerTarget = context.DataAccess.ExecuteReader(sqlTarget);

            if (!readerTarget.Read())
                throw new InvalidOperationException("Empty target reader.");
            var targetValues1 = new object[readerTarget.FieldCount];
            readerTarget.GetValues(targetValues1);

            if (!readerTarget.Read())
                throw new InvalidOperationException("Empty target reader.");
            var targetValues2 = new object[readerTarget.FieldCount];
            readerTarget.GetValues(targetValues2);

            context.DataAccess.CloseConnection();

            AllDataTypesUtil.CompareSourceAndTargetValues(sourceValues1, targetValues1);
            AllDataTypesUtil.CompareSourceAndTargetValues(sourceValues2, targetValues2);
        }

        protected override void Teardown(TeardownContext context)
        {
            context.DataAccess.OpenConnection(Constants.DbConnectionString);
            context.DataAccess.ExecuteNonQuery("DELETE FROM [dbo].[AllDataTypesSource]");
            context.DataAccess.ExecuteNonQuery("DELETE FROM [dbo].[AllDataTypesTarget]");
            context.DataAccess.CloseConnection();
        }
    }

    [UnitTest("DataTypes", "DataTypes.dtsx")]
    [FakeDestination(@"\[DataTypes]\[Data Flow Task]\[OLEDB All data target]")]
    public class AllDataTypes2 : BaseUnitTest
    {
        protected override void Setup(SetupContext context)
        {
            context.Package.GetConnection("CustomerDB").SetConnectionString(Constants.SsisDbConnectionString);

            string sql = AllDataTypesUtil.GetInsertSql("[dbo].[AllDataTypesSource]");
            context.DataAccess.OpenConnection(Constants.DbConnectionString);
            context.DataAccess.ExecuteNonQuery("DELETE FROM [dbo].[AllDataTypesSource]");
            context.DataAccess.ExecuteNonQuery("DELETE FROM [dbo].[AllDataTypesTarget]");
            context.DataAccess.ExecuteNonQuery(sql);
            context.DataAccess.CloseConnection();
        }

        protected override void Verify(VerificationContext context)
        {
            context.DataAccess.OpenConnection(Constants.DbConnectionString);
            var cnt = (int)context.DataAccess.ExecuteScalar("SELECT COUNT(*) FROM [dbo].[AllDataTypesTarget]");
            context.DataAccess.CloseConnection();
            Assert.AreEqual(0, cnt);

            AllDataTypesUtil.CheckFakeDestination(context);
        }

        protected override void Teardown(TeardownContext context)
        {
            context.DataAccess.OpenConnection(Constants.DbConnectionString);
            context.DataAccess.ExecuteNonQuery("DELETE FROM [dbo].[AllDataTypesSource]");
            context.DataAccess.ExecuteNonQuery("DELETE FROM [dbo].[AllDataTypesTarget]");
            context.DataAccess.CloseConnection();
        }
    }

    [UnitTest("DataTypes", "DataTypes.dtsx")]
    [DataTap(@"\[DataTypes]\[Data Flow Task]\[OLEDB All data source]", @"\[DataTypes]\[Data Flow Task]\[OLEDB All data target]")]
    public class AllDataTypes3 : BaseUnitTest
    {
        protected override void Setup(SetupContext context)
        {
            context.Package.GetConnection("CustomerDB").SetConnectionString(Constants.SsisDbConnectionString);

            string sql = AllDataTypesUtil.GetInsertSql("[dbo].[AllDataTypesSource]");
            context.DataAccess.OpenConnection(Constants.DbConnectionString);
            context.DataAccess.ExecuteNonQuery("DELETE FROM [dbo].[AllDataTypesSource]");
            context.DataAccess.ExecuteNonQuery("DELETE FROM [dbo].[AllDataTypesTarget]");
            context.DataAccess.ExecuteNonQuery(sql);
            context.DataAccess.CloseConnection();
        }

        protected override void Verify(VerificationContext context)
        {
            AllDataTypesUtil.CheckDataTap(context);
        }

        protected override void Teardown(TeardownContext context)
        {
            context.DataAccess.OpenConnection(Constants.DbConnectionString);
            context.DataAccess.ExecuteNonQuery("DELETE FROM [dbo].[AllDataTypesSource]");
            context.DataAccess.ExecuteNonQuery("DELETE FROM [dbo].[AllDataTypesTarget]");
            context.DataAccess.CloseConnection();
        }
    }

    [UnitTest("DataTypes", "DataTypes.dtsx")]
    [FakeSource(@"\[DataTypes]\[Data Flow Task]\[OLEDB All data source]",
        new[] { "OLE DB Source Output" },
        new[]
        {
            "-9223372036854775808;-2147483648;-32768;0;-922337203685477.5808;-214748.3648;-922337.8547;-922337.8547;-9.223372E+14;-9.223372E+14;1900-01-01;1900-01-01 13:45:00.000;1900-01-01 13:45:00.0000000;1900-01-01 00:00:00;00:00:00.0000000;0001-01-01 00:00:00 +00:00;qwertzuiop1234567890;üöäqwertzuiop1234567890;qwertzuiop;üöäqwertzu;Some text.;Some text.;FAAAAdIKH+uMqQ==;FAAAAdIKH+uMqVSr;FAAAAdIKH+uMqVSr;False\r\n9223372036854775807;2147483647;32767;255;922337203685477.5807;214748.3647;922337.8547;922337.8547;9.223372E+14;9.223372E+14;2100-01-01;2100-01-01 16:45:00.000;2100-01-01 16:45:00.0000000;2079-06-06 00:00:00;23:59:59.9999999;9999-12-31 00:00:00 +00:00;qwertzuiop1234567890asdfghjkl1234567890yxcvbnm1234567890;üöäqwertzuiop1234567890asdfghjkl1234567890yxcvbnm1234567890;qwertzuiop;üöäqwertzu;More text without meaning.;More text without meaning.;JgAAAU7zON5QkA==;EjRWeJASNFZ4kBI0VniQEjRWeJASNFZ4kBI0VniQEjRWeJASNFZ4kBI0VniQEjRWeJASNFZ4kBI0VniQEjRWeJASNFZ4kBI0VniQEjRWeJA=;EjRWeJASNFZ4kBI0VniQEjRWeJASNFZ4kBI0VniQEjRWeJASNFZ4kBI0VniQEjRWeJASNFZ4kBI0VniQEjRWeJASNFZ4kBI0VniQEjRWeJA=;True"
        },
        RowDelimiter = "\r\n", ColumnDelimiter = ";")]
    [FakeDestination(@"\[DataTypes]\[Data Flow Task]\[OLEDB All data target]")]
    [DataTap(@"\[DataTypes]\[Data Flow Task]\[OLEDB All data source]", @"\[DataTypes]\[Data Flow Task]\[OLEDB All data target]")]
    public class AllDataTypes4 : BaseUnitTest
    {
        protected override void Setup(SetupContext context)
        {
            context.Package.GetConnection("CustomerDB").SetConnectionString(Constants.SsisDbConnectionString);

            string sql = AllDataTypesUtil.GetInsertSql("[dbo].[AllDataTypesSource]");
            context.DataAccess.OpenConnection(Constants.DbConnectionString);
            context.DataAccess.ExecuteNonQuery("DELETE FROM [dbo].[AllDataTypesSource]");
            context.DataAccess.ExecuteNonQuery("DELETE FROM [dbo].[AllDataTypesTarget]");
            context.DataAccess.ExecuteNonQuery(sql);
            context.DataAccess.CloseConnection();
        }

        protected override void Verify(VerificationContext context)
        {
            context.DataAccess.OpenConnection(Constants.DbConnectionString);
            var cnt = (int)context.DataAccess.ExecuteScalar("SELECT COUNT(*) FROM [dbo].[AllDataTypesTarget]");
            context.DataAccess.CloseConnection();
            Assert.AreEqual(0, cnt);

            AllDataTypesUtil.CheckFakeDestination(context);
            AllDataTypesUtil.CheckDataTap(context);
        }

        protected override void Teardown(TeardownContext context)
        {
            context.DataAccess.OpenConnection(Constants.DbConnectionString);
            context.DataAccess.ExecuteNonQuery("DELETE FROM [dbo].[AllDataTypesSource]");
            context.DataAccess.ExecuteNonQuery("DELETE FROM [dbo].[AllDataTypesTarget]");
            context.DataAccess.CloseConnection();
        }
    }

    class AllDataTypesUtil
    {
        internal static string GetInsertSql(string tableName)
        {
            string sql = File.ReadAllText(Constants.PathToInsertAllDataTypesSql);
            return sql.Replace("#TargetTable", tableName);
        }

        internal static void CompareSourceAndTargetValues(object[] sourceValues, object[] targetValues)
        {
            Assert.AreEqual(sourceValues.Length, targetValues.Length);

            for (int i = 0; i < sourceValues.Length; i++)
            {
                object sourceValue = sourceValues[i];
                object targetValue = targetValues[i];

                if (sourceValue is byte[])
                {
                    var src = (byte[])sourceValue;
                    var tgt = (byte[])targetValue;

                    Assert.AreEqual(src.Length, tgt.Length);

                    for (int j = 0; j < src.Length; j++)
                    {
                        byte srcVal = src[j];
                        byte tgtVal = tgt[j];

                        Assert.AreEqual(srcVal, tgtVal);
                    }
                }
                else
                    Assert.AreEqual(sourceValue, targetValue);
            }
        }

        internal static object[] ConvertStringsToValues(string[] row)
        {
            var values = new object[row.Length];
            int i = 0;
            values[i] = long.Parse(row[i], NumberStyles.Integer, CultureInfo.InvariantCulture);
            values[++i] = int.Parse(row[i], NumberStyles.Integer, CultureInfo.InvariantCulture);
            values[++i] = short.Parse(row[i], NumberStyles.Integer, CultureInfo.InvariantCulture);
            values[++i] = byte.Parse(row[i], NumberStyles.Integer, CultureInfo.InvariantCulture);

            values[++i] = decimal.Parse(row[i], NumberStyles.Float, CultureInfo.InvariantCulture);
            values[++i] = decimal.Parse(row[i], NumberStyles.Float, CultureInfo.InvariantCulture);
            values[++i] = decimal.Parse(row[i], NumberStyles.Float, CultureInfo.InvariantCulture);
            values[++i] = decimal.Parse(row[i], NumberStyles.Float, CultureInfo.InvariantCulture);
            values[++i] = float.Parse(row[i], NumberStyles.Float, CultureInfo.InvariantCulture);
            values[++i] = float.Parse(row[i], NumberStyles.Float, CultureInfo.InvariantCulture);

            values[++i] = DateTime.Parse(row[i], CultureInfo.InvariantCulture);
            values[++i] = DateTime.Parse(row[i], CultureInfo.InvariantCulture);
            values[++i] = DateTime.Parse(row[i], CultureInfo.InvariantCulture);
            values[++i] = DateTime.Parse(row[i], CultureInfo.InvariantCulture);
            values[++i] = TimeSpan.Parse(row[i]);
            values[++i] = DateTimeOffset.Parse(row[i], CultureInfo.InvariantCulture);

            values[++i] = row[i];
            values[++i] = row[i];
            values[++i] = row[i];
            values[++i] = row[i];
            values[++i] = row[i];
            values[++i] = row[i];

            values[++i] = Convert.FromBase64String(row[i]);
            values[++i] = Convert.FromBase64String(row[i]);
            values[++i] = Convert.FromBase64String(row[i]);

            values[++i] = bool.Parse(row[i]);

            return values;
        }

        internal static void CheckFakeDestination(VerificationContext context)
        {
            FakeDestination fakeDestination = context.FakeDestinations[0];

            foreach (FakeDestinationSnapshot snapshot in fakeDestination.Snapshots)
            {
                string data = snapshot.LoadData();
                Assert.IsTrue(!string.IsNullOrEmpty(data));

                string[] rows = data.Split(new[] { "\r\n" }, StringSplitOptions.RemoveEmptyEntries);
                Assert.AreEqual(2, rows.Length);

                object[] fakeDestinationValues1 =
                    ConvertStringsToValues(rows[0].TrimEnd(';').Split(';'));
                object[] fakeDestinationValues2 =
                    ConvertStringsToValues(rows[1].TrimEnd(';').Split(';'));

                // Source              
                context.DataAccess.OpenConnection(Constants.DbConnectionString);

                const string sqlSource = "SELECT * FROM [dbo].[AllDataTypesSource]";

                SqlDataReader readerSource = context.DataAccess.ExecuteReader(sqlSource);

                if (!readerSource.Read())
                    throw new InvalidOperationException("Empty source reader.");
                var sourceValues1 = new object[readerSource.FieldCount];
                readerSource.GetValues(sourceValues1);

                if (!readerSource.Read())
                    throw new InvalidOperationException("Empty source reader.");
                var sourceValues2 = new object[readerSource.FieldCount];
                readerSource.GetValues(sourceValues2);

                context.DataAccess.CloseConnection();

                CompareSourceAndTargetValues(fakeDestinationValues1, sourceValues1);
                CompareSourceAndTargetValues(fakeDestinationValues2, sourceValues2);
            }
        }

        internal static void CheckDataTap(VerificationContext context)
        {
            DataTap dataTap = context.DataTaps[0];

            Assert.AreEqual(dataTap.Snapshots.Count, dataTap.SnapshotCount);
            Assert.AreEqual(1, dataTap.SnapshotCount);

            foreach (DataTapSnapshot snapshot in dataTap.Snapshots)
            {
                string data = snapshot.LoadData();
                Assert.IsTrue(!string.IsNullOrEmpty(data));

                string[] rows = data.Split(new[] { "\r\n" }, StringSplitOptions.RemoveEmptyEntries);
                Assert.AreEqual(2, rows.Length);

                object[] fakeDestinationValues1 =
                    ConvertStringsToValues(rows[0].TrimEnd(';').Split(';'));
                object[] fakeDestinationValues2 =
                    ConvertStringsToValues(rows[1].TrimEnd(';').Split(';'));

                // Source              
                context.DataAccess.OpenConnection(Constants.DbConnectionString);

                const string sqlSource = "SELECT * FROM [dbo].[AllDataTypesSource]";

                SqlDataReader readerSource = context.DataAccess.ExecuteReader(sqlSource);

                if (!readerSource.Read())
                    throw new InvalidOperationException("Empty source reader.");
                var sourceValues1 = new object[readerSource.FieldCount];
                readerSource.GetValues(sourceValues1);

                if (!readerSource.Read())
                    throw new InvalidOperationException("Empty source reader.");
                var sourceValues2 = new object[readerSource.FieldCount];
                readerSource.GetValues(sourceValues2);

                context.DataAccess.CloseConnection();

                CompareSourceAndTargetValues(fakeDestinationValues1, sourceValues1);
                CompareSourceAndTargetValues(fakeDestinationValues2, sourceValues2);
            }
        }
    }
}
