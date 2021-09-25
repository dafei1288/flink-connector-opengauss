package name.lijiaqi.table;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.logical.RowType;

/**
 * @author lijiaqi
 */
public class OpenGaussDynamicTableSource implements ScanTableSource {

    private final JdbcOptions options;
    private final TableSchema tableSchema;

    public OpenGaussDynamicTableSource(JdbcOptions options, TableSchema tableSchema) {
        this.options = options;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    @SuppressWarnings("unchecked")
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

        final JdbcDialect dialect = options.getDialect();

        String query = dialect.getSelectFromStatement(
                options.getTableName(), tableSchema.getFieldNames(), new String[0]);

        final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();

        final JdbcRowDataInputFormat.Builder builder = JdbcRowDataInputFormat.builder()
                .setDrivername(options.getDriverName())
                .setDBUrl(options.getDbURL())
                .setUsername(options.getUsername().orElse(null))
                .setPassword(options.getPassword().orElse(null))
                .setQuery(query)
                .setRowConverter(dialect.getRowConverter(rowType))
                //(TypeInformation<RowData>)
                .setRowDataTypeInfo( runtimeProviderContext
                        .createTypeInformation(tableSchema.toRowDataType()));

        return InputFormatProvider.of(builder.build());

    }

    @Override
    public DynamicTableSource copy() {
        return new OpenGaussDynamicTableSource(options, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "OpenGauss Table Source";
    }

}
