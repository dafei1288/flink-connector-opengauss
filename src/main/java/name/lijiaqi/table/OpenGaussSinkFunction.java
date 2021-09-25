package name.lijiaqi.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * @author lijiaqi
 */
public class OpenGaussSinkFunction extends RichSinkFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private static final String MAX_PARALLEL_REPLICAS_VALUE = "2";

    private final JdbcOptions jdbcOptions;
    private final SerializationSchema<RowData> serializationSchema = null;
    private DataType dateType;

    private Connection conn;
    private Statement stmt;
//    private String sql  = "insert into";

    public OpenGaussSinkFunction(JdbcOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
//        this.serializationSchema = serializationSchema;
    }

    public OpenGaussSinkFunction(JdbcOptions jdbcOptions, DataType dataType) {
        this.jdbcOptions = jdbcOptions;
        this.dateType = dataType;
    }

    @Override
    public void open(Configuration parameters) {
        System.out.println("open connection !!!!!");
//        ClickHouseProperties properties = new ClickHouseProperties();
//        properties.setUser(jdbcOptions.getUsername().orElse(null));
//        properties.setPassword(jdbcOptions.getPassword().orElse(null));
//        BalancedClickhouseDataSource dataSource;
        try {
            if (null == conn) {
                Class.forName(jdbcOptions.getDriverName());
                conn = DriverManager.getConnection(jdbcOptions.getDbURL(),jdbcOptions.getUsername().orElse(null),jdbcOptions.getPassword().orElse(null));
//                conn = dataSource.getConnection();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {

//        System.out.println("into ....");
        try {
//        byte[] serialize = serializationSchema.serialize(value);
            stmt = conn.createStatement();
            String sql = "insert into " + this.jdbcOptions.getTableName() + " values ( ";
            for (int i = 0; i < value.getArity(); i++) {
//                try {
//                    System.out.println(dateType.getChildren().get(i).getConversionClass().getName());
//                    if(i==0){
//                        sql += +value.getInt(i)+ " ,";
//                    }else{
//                        sql += "'"+value.getString(i) + "' ,";
//                    }
//                }catch (Exception e){
//                    e.printStackTrace();
//                }

                if(dateType.getChildren().get(i).getConversionClass().equals(Integer.class)){
                    sql += +value.getInt(i)+ " ,";
                }else {
                    sql += "'"+value.getString(i) + "' ,";
                }



            }
            sql = sql.substring(0, sql.length() - 1);
            sql += " ); ";

            System.out.println("sql ==>" + sql);

            stmt.execute(sql);
        }catch(Exception e){
            e.printStackTrace();
        }
//        stmt.write().table(jdbcOptions.getTableName()).data(new ByteArrayInputStream(serialize), ClickHouseFormat.JSONEachRow)
//                .addDbParam(ClickHouseQueryParam.MAX_PARALLEL_REPLICAS, MAX_PARALLEL_REPLICAS_VALUE).send();
    }

    @Override
    public void close() throws Exception {
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

}
