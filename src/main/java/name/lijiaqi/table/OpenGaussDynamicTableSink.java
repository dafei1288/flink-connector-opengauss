package name.lijiaqi.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * @author lijiaqi
 */
public class OpenGaussDynamicTableSink implements DynamicTableSink {

    private final JdbcOptions jdbcOptions;
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private final DataType dataType;

    public OpenGaussDynamicTableSink(JdbcOptions jdbcOptions, EncodingFormat<SerializationSchema<RowData>> encodingFormat, DataType dataType) {
        this.jdbcOptions = jdbcOptions;
        this.encodingFormat = encodingFormat;
        this.dataType = dataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        System.out.println("SinkRuntimeProvider");
        System.out.println(dataType);

//        SerializationSchema<RowData> serializationSchema = encodingFormat.createRuntimeEncoder(context, dataType);
        OpenGaussSinkFunction gbasedbtSinkFunction = new OpenGaussSinkFunction(jdbcOptions,dataType);
        return SinkFunctionProvider.of(gbasedbtSinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new OpenGaussDynamicTableSink(jdbcOptions, encodingFormat, dataType);
    }

    @Override
    public String asSummaryString() {
        return "OpenGauss Table Sink";
    }

}
