using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;

namespace Amop.Core.Helpers
{
    public class SQLBulkCopyHelper
    {
        public static List<SqlBulkCopyColumnMapping> AutoMapColumns(DataTable dataTable)
        {
            var columnMappings = new List<SqlBulkCopyColumnMapping>();
            foreach (DataColumn dataColumn in dataTable.Columns)
            {
                columnMappings.Add(new SqlBulkCopyColumnMapping(dataColumn.ColumnName, dataColumn.ColumnName));
            }
            return columnMappings;
        }
    }
}
