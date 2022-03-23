import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.TreeMap;

public class ConstantInvestFundsManager extends InvestFundsManager {

    public static final int NIL = -1;

    private final List<BigDecimal> RENTABILIDADE_DIARIA_FUNDO;
    private final List<BigDecimal> RENTABILIDADE_DIARIA_MERCADO;
    private final BigDecimal VOLATILIDADE_MERCADO;
    private final int NUM_VALORES;

    public ConstantInvestFundsManager(List<BigDecimal> rentabilidadeDiariaFundo, List<BigDecimal> rentabilidadeDiariaMercado, BigDecimal volatilidadeMercado) throws ClassNotFoundException, SQLException {
        assert(rentabilidadeDiariaFundo.size() == rentabilidadeDiariaMercado.size());
        this.RENTABILIDADE_DIARIA_FUNDO = rentabilidadeDiariaFundo;
        this.RENTABILIDADE_DIARIA_MERCADO = rentabilidadeDiariaMercado;
        this.VOLATILIDADE_MERCADO = volatilidadeMercado;
        this.NUM_VALORES = rentabilidadeDiariaFundo.size();
    }

    @Override
    protected void init_cache_mercado(Connection connection) throws SQLException {
        this.rentabilidadeDiariaMercadoCache.put(NIL, new TreeMap<>());
        this.rentabilidadeMediaMercadoCache.put(NIL, new TreeMap<>());
        this.desvioPadraoMercadoCache.put(NIL, new TreeMap<>());
    }

    @Override
    protected List<BigDecimal> rentabilidade_daria_fundo(Connection connection, int idFundo, String data, int numValores) throws SQLException {
        return this.RENTABILIDADE_DIARIA_FUNDO;
    }

    @Override
    protected List<BigDecimal> rentabilidade_diaria_mercado(Connection connection, int tipoBenchmarkId, String dataFinal, int numValores) throws SQLException {
        return this.RENTABILIDADE_DIARIA_MERCADO;
    }

    @Override
    protected BigDecimal volatilidade_mercado(Connection connection, int tipoBenchmarkId, String dataFinal, int numValores) throws SQLException {
        return this.VOLATILIDADE_MERCADO;
    }

    public BigDecimal calcular_sharpe_generalizado_fundo() throws SQLException {
        return this.sharpe_generalizado_fundo(null, NIL, NIL, null, this.NUM_VALORES);
    }

    public BigDecimal calcular_sharpe_fundo() throws SQLException {
        return this.sharpe_fundo(null, NIL, NIL, null, this.NUM_VALORES);
    }

    public BigDecimal calcular_beta_fundo() throws SQLException {
        return this.beta_fundo(null, NIL, NIL, null, this.NUM_VALORES);
    }

    public BigDecimal calcular_rentabilidade_media_fundo() throws SQLException {
        return this.rentabilidade_media_fundo(null, NIL, null, this.NUM_VALORES);
    }

    public BigDecimal calcular_volatilidade_fundo() throws SQLException {
        return this.desvio_padrao_fundo(null, NIL, null, this.NUM_VALORES);
    }

    public BigDecimal calcular_rentabilidade_media_mercado() throws  SQLException {
        return this.rentabilidade_media_mercado(null, NIL, null, this.NUM_VALORES);
    }
}