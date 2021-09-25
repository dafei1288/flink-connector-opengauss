package name.lijiaqi.converter;

import org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

/**
 * @author lijiaqi
 */
public class OpenGaussRowConverter extends AbstractJdbcRowConverter {

    public OpenGaussRowConverter(RowType rowType) {
        super(rowType);
    }

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "opengauss";
    }

}