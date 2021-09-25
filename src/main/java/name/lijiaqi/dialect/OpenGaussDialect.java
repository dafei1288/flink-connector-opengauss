package name.lijiaqi.dialect;

import name.lijiaqi.converter.OpenGaussRowConverter;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;

import java.util.Optional;

/**
 *
 * @author lijiaqi
 */
public class OpenGaussDialect implements JdbcDialect {

    private static final long serialVersionUID = 1L;

    @Override
    public String dialectName() {
        return "opengauss";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:opengauss:");
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new OpenGaussRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long l) {
        return null;
    }

    @Override
    public void validate(TableSchema schema) throws ValidationException {
        JdbcDialect.super.validate(schema);
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.opengauss.Driver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "'" + identifier + "'";
    }

    @Override
    public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return JdbcDialect.super.getUpsertStatement(tableName, fieldNames, uniqueKeyFields);
    }

    @Override
    public String getRowExistsStatement(String tableName, String[] conditionFields) {
        return JdbcDialect.super.getRowExistsStatement(tableName, conditionFields);
    }

    @Override
    public String getInsertIntoStatement(String tableName, String[] fieldNames) {
        return JdbcDialect.super.getInsertIntoStatement(tableName, fieldNames);
    }

    @Override
    public String getUpdateStatement(String tableName, String[] fieldNames, String[] conditionFields) {
        return JdbcDialect.super.getUpdateStatement(tableName, fieldNames, conditionFields);
    }

    @Override
    public String getDeleteStatement(String tableName, String[] conditionFields) {
        return JdbcDialect.super.getDeleteStatement(tableName, conditionFields);
    }

    @Override
    public String getSelectFromStatement(String tableName, String[] selectFields, String[] conditionFields) {
        return JdbcDialect.super.getSelectFromStatement(tableName, selectFields, conditionFields);
    }

}
