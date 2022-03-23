import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Month;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InvestFundsManager {
    private final String databaseName;
    private final String databaseUsername;
    private final String databasePassword;
    private final String JDBC_DRIVER;
    private final String DB_URL;
    private final String DB_PROPERTIES;
    private final boolean DEBUG = true;
    private final TipoAtualizacao ATUALIZACAO = TipoAtualizacao.COMPLETA;

    private Connection connection;
    private final String datafilename;

    public InvestFundsManager() throws SQLException, ClassNotFoundException {
        this(false);
    }

    public InvestFundsManager(boolean connectToDB) throws ClassNotFoundException, SQLException {
        databaseName = "investFunds";
        databaseUsername = "<usuário>";
        databasePassword = "<senha>";
        JDBC_DRIVER = "com.mysql.jdbc.Driver";
        DB_PROPERTIES = "?useSSL=false";
        DB_URL = "jdbc:mysql://localhost:3306/";
        datafilename = "<id-do-fundo>";
        this.rentabilidadeDiariaMercadoCache = new TreeMap<>();
        this.rentabilidadeMediaMercadoCache = new TreeMap<>();
        this.desvioPadraoMercadoCache = new TreeMap<>();
        if (connectToDB) {
            ConnectToDB();
        }
        init_cache_mercado(connection);
    }

    /**
     * Precisão depois do ponto definido pelo tipo DECIMAL(27, 12) utilizada no banco
     */
    private static final MathContext PRECISAO_DECIMAL = new MathContext(64);
    /**
     * Formatador de data, utilizado para converter datas em String no banco para instâncias de Data (vice-versa)
     */
    private static final SimpleDateFormat formatadorData = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * Cache de rentabilidade diária do mercado
     */
    protected final Map<Integer, Map<String, BigDecimal>> rentabilidadeDiariaMercadoCache;
    /**
     * Cache de rentabilidade média do mercado
     */
    protected final Map<Integer, Map<Pair<String, Integer>, BigDecimal>> rentabilidadeMediaMercadoCache;
    /**
     * Cache de volatibilidade do mercado
     */
    protected final Map<Integer, Map<Pair<String, Integer>, BigDecimal>> desvioPadraoMercadoCache;

    /**
     * Inicia a cache de indicadores de mercado. Evita recalcular estes indicadores, que
     * podem necessitar de centenas ou milhares de operações. Inicia-se uma cache para cada
     * indicador e para cada mercado. Os mercados (benchmarks) são lidos do banco de dados.
     * @param connection Conexão com o banco de dados.
     * @throws SQLException Quando não é possível acessar o banco.
     */
    protected void init_cache_mercado(Connection connection) throws SQLException {
        String sql;
        Statement st = connection.createStatement();
        java.sql.ResultSet rs;
        // Busca IDs dos mercados
        sql = "SELECT benchmark_id FROM tipo_benchmarks";
        rs = st.executeQuery(sql);
        while (rs.next()) {
            int benchmarkId = rs.getInt("benchmark_id");
            this.rentabilidadeDiariaMercadoCache.put(benchmarkId, new TreeMap<>());
            this.rentabilidadeMediaMercadoCache.put(benchmarkId, new TreeMap<>());
            this.desvioPadraoMercadoCache.put(benchmarkId, new TreeMap<>());
        }
    }

    /* mysql> DESCRIBE doc_inf_diario_fundos;
     *        +------------------+----------------+------+-----+---------+-------+
     *        | Field            | Type           | Null | Key | Default | Extra |
     *        +------------------+----------------+------+-----+---------+-------+
     *        | cnpj_fundo_id    | int(11)        | NO   | PRI | NULL    |       |
     *        | DT_COMPTC        | date           | NO   | PRI | NULL    |       |
     *        | VL_TOTAL         | decimal(17,2)  | NO   |     | NULL    |       |
     *        | VL_QUOTA         | decimal(27,12) | NO   |     | NULL    |       |
     *        | VL_PATRIM_LIQ    | decimal(17,2)  | NO   |     | NULL    |       |
     *        | CAPTC_DIA        | decimal(17,2)  | NO   |     | NULL    |       |
     *        | RESG_DIA         | decimal(17,2)  | NO   |     | NULL    |       |
     *        | NR_COTST         | int(10)        | NO   |     | NULL    |       |
     *        | rentab_diaria    | decimal(27,12) | YES  |     | NULL    |       |
     *        | volat_diaria     | decimal(27,12) | YES  |     | NULL    |       |
     *        | rentab_acumulada | decimal(27,12) | YES  |     | NULL    |       |
     *        | drawdown         | decimal(27,12) | YES  |     | NULL    |       |
     *        +------------------+----------------+------+-----+---------+-------+
     */
    /*
     * (APÓS A EXECUÇÃO DO SCRIPT ./src/main/resources/benchmark.sql)
     * mysql> DESCRIBE indicadores_fundos;
     *        +-------------------+----------------+------+-----+---------+-------+
     *        | Field             | Type           | Null | Key | Default | Extra |
     *        +-------------------+----------------+------+-----+---------+-------+
     *        | cnpj_fundo_id     | int(11)        | NO   | PRI | NULL    |       |
     *        | periodo_meses     | int(11)        | NO   | PRI | NULL    |       |
     *        | data_final        | date           | NO   | PRI | NULL    |       |
     *        | rentabilidade     | decimal(27,12) | NO   |     | NULL    |       |
     *        | desvio_padrao     | decimal(27,12) | NO   |     | NULL    |       |
     *        | num_valores       | int(11)        | YES  |     | NULL    |       |
     *        | rentab_min        | decimal(27,12) | YES  |     | NULL    |       |
     *        | rentab_max        | decimal(27,12) | YES  |     | NULL    |       |
     *        | max_drawdown      | decimal(27,12) | NO   |     | NULL    |       |
     *        | tipo_benchmark_id | int(11)        | YES  | MUL | NULL    |       |
     *        | meses_acima_bench | int(11)        | YES  |     | NULL    |       |
     *        | sharpe            | decimal(27,12) | YES  |     | NULL    |       |
     *        | beta              | decimal(27,12) | YES  |     | NULL    |       |
     *        +-------------------+----------------+------+-----+---------+-------+
     */
    /*
     * mysql> DESCRIBE tipo_benchmarks;
     *        +----------------+--------------+------+-----+---------+----------------+
     *        | Field          | Type         | Null | Key | Default | Extra          |
     *        +----------------+--------------+------+-----+---------+----------------+
     *        | benchmark_id   | int(11)      | NO   | PRI | NULL    | auto_increment |
     *        | benchmark_name | varchar(255) | NO   |     | NULL    |                |
     *        +----------------+--------------+------+-----+---------+----------------+
     */
    /*
     * mysql> SELECT * FROM tipo_benchmarks;
     *        +--------------+----------------+
     *        | benchmark_id | benchmark_name |
     *        +--------------+----------------+
     *        |            1 | SELIC          |
     *        |            2 | CDI            |
     *        |            3 | IPCA           |
     *        |            4 | IBOVESPA       |
     *        |            5 | USD/BRL        |
     *        +--------------+----------------+
     */

    /**
     * Abre uma conexão com o banco de dados com base nos dados iniciados no construtor da classe.
     * @throws ClassNotFoundException Quando não consegue encontrar o DRIVE utilizado na conexão com o banco.
     * @throws SQLException Quando não é possível abrir uma conexão com o banco.
     */
    private void ConnectToDB() throws ClassNotFoundException, SQLException {
        Class.forName(this.JDBC_DRIVER);
        connection = DriverManager.getConnection(this.DB_URL + this.databaseName + this.DB_PROPERTIES, this.databaseUsername, this.databasePassword);
        connection.setAutoCommit(false);
    }

    /**
     * Calcula a rentabilidade diária conforme as cotas armazenadas no Banco.
     * @param connection Conexão com o banco de dados.
     * @param idFundo ID do fundo
     * @param execucao Gerador de estatística da execução
     * @throws SQLException Quando não é possível acessar o banco de dados.
     */
    private void calcula_rentabilidade_diaria(Connection connection, int idFundo, ExecutionInformation execucao) throws SQLException {
        String sql;
        Statement st = connection.createStatement();
        Statement st2 = connection.createStatement();
        java.sql.ResultSet rs;
        java.sql.ResultSet rs2;
        /*
         * Calcula as rentabilidades diárias
         */
        sql = "SELECT VL_QUOTA, DT_COMPTC FROM doc_inf_diario_fundos WHERE cnpj_fundo_id=" + idFundo + " and (volat_diaria is null) order by DT_COMPTC ASC"; // registros sem rentabilidade, para calcular
        rs = st.executeQuery(sql);
        if (rs.next()) {
            Double valorQuotaAnterior, rentabAcumulAnterior, valorQuotaAtual, rentabilidade, volatilidade, rentabilidadeAcumulada, maxValorQuota, drawdown, temp;//, somaRent;
            Double rent1, rent2 = Double.NaN, rent3 = Double.NaN;
            String dataAtual;
            String firstEmptyDate = rs.getDate("DT_COMPTC").toString(); // primeira data sem rentabilidade calculada. Tenta buscar a última rentabilidade calculada antes dessa
            sql = "SELECT VL_QUOTA, rentab_acumulada FROM doc_inf_diario_fundos WHERE cnpj_fundo_id=" + idFundo + " AND (volat_diaria IS NOT NULL) AND DT_COMPTC<'" + firstEmptyDate + "' ORDER BY DT_COMPTC DESC";
            rs2 = st2.executeQuery(sql);
            if (rs2.next()) { // Há valores anteriores à faixa de valores vazios. Pega o último valor de cota conhecido antes disso
                valorQuotaAnterior = rs2.getDouble("VL_QUOTA");
                rentabAcumulAnterior = rs2.getDouble("rentab_acumulada");
                sql = "SELECT MAX(VL_QUOTA) AS MAX_VL_QUOTA FROM doc_inf_diario_fundos WHERE cnpj_fundo_id=" + idFundo + " AND DT_COMPTC<'" + firstEmptyDate + "' ORDER BY DT_COMPTC DESC";
                rs2 = st2.executeQuery(sql);
                if (rs2.next()) {
                    maxValorQuota = rs2.getDouble("MAX_VL_QUOTA");
                } else {
                    // nuncA deveria acontecer
                    maxValorQuota = 0.0; //?????
                }
            } else {
                // Não há valores anteriores. Então o valor anterior deve ser o primeiro valor da quota no período sem valores calculados
                valorQuotaAnterior = rs.getDouble("VL_QUOTA");
                rentabAcumulAnterior = 0.0;
                maxValorQuota = valorQuotaAnterior;
            }
            // Tendo um valor anterior, varre todo o período calculando a rentabilidade
            do {
                // Calcula rentabilidades
                valorQuotaAtual = rs.getDouble("VL_QUOTA");
                dataAtual = rs.getDate("DT_COMPTC").toString();
                if (valorQuotaAnterior != 0.0) {
                    rentabilidade = (valorQuotaAtual - valorQuotaAnterior) / valorQuotaAnterior;
                } else {
                    rentabilidade = 0.0;
                }
                if (rentabilidade.isNaN() || rentabilidade.isInfinite()) {
                    rentabilidade = 0.0;
                }
                rentabilidadeAcumulada = rentabAcumulAnterior + rentabilidade;
                // calcula volatilidade
                rent1 = rent2;
                rent2 = rent3;
                rent3 = rentabilidade;
                if (!rent1.isNaN()) {
                    temp = (rent1 + rent2 + rent3) / 3; // TODO: Conferir: a volatilidade diária é calculada com base numa média dos últimos 3 dias.
                    volatilidade = Math.sqrt((Math.pow(rent1 - temp, 2) + Math.pow(rent2 - temp, 2) + Math.pow(rent3 - temp, 2)) / 2);//2=n-1
                } else {
                    volatilidade = 0.0;
                }

                // calcula drawdown
                if (valorQuotaAtual <= maxValorQuota) {
                    drawdown = valorQuotaAtual / maxValorQuota - 1.0;
                } else {
                    maxValorQuota = valorQuotaAtual;
                    drawdown = 0.0;
                }

                // salva no BD
                sql = "UPDATE doc_inf_diario_fundos SET rentab_diaria=" + rentabilidade + ", rentab_acumulada=" + rentabilidadeAcumulada + ", volat_diaria=" + volatilidade + ", drawdown=" + drawdown + " WHERE cnpj_fundo_id=" + idFundo + " AND DT_COMPTC='" + dataAtual + "'";
                st2.execute(sql);
                execucao.incrementCountRecords();

                if (execucao.getCountRecords() % 1e2 == 0) {
                    if (!DEBUG) { connection.commit(); }
                    if (execucao.getCountRecords() % 1e3 == 0) {
                        long tempoAtual = System.currentTimeMillis();
                        execucao.setTempoProcessamentoParte(tempoAtual - execucao.getFimProcessamento());
                        execucao.setTempoProcessamentoTotal(tempoAtual - execucao.getInicioProcessamento());
                        System.out.println("info: " + execucao.getCountRecords() / 1e3 + "K registros processados até o momento (totalizando " + execucao.getTempoProcessamentoTotal().intValue() + " ms)");//, sendo " + countRecordsComErro + " com erro e " + countRecordsRepetidos + " preexistentes");
                        execucao.setFimProcessamento(tempoAtual);
                    }
                }

                // atualiza vars
                valorQuotaAnterior = valorQuotaAtual;
                rentabAcumulAnterior = rentabilidadeAcumulada;
            } while (rs.next());
            if (!DEBUG) { connection.commit(); }
        } else {
            // não há valores vazios a serem calculados nos indicadores diários. Estão todos calculados e prontos. Não há o que fazer (ou simplesmente não há qualquer registro nos informativos diários
            execucao.incrementCountRecordsRepetido();
        }
    }

    /**
     * Calcula indicadores (com exemção de indicadores de mercados) de um fundo. Resultados são armazenados na tabela
     * 'indicadores_fundos'. Os indicadores calculados são rentabilidade, desvio padrao, rentabilidade mínima,
     *  rentabilidade máxima e drawdown máximo.
     * @param connection Conexão com o banco de dados
     * @param idFundo ID do fundo
     * @param execucao Gerador de estatística da execução
     * @throws SQLException Quando não é possível acessar o banco
     */
    private void calcula_indicadores(Connection connection, int idFundo, ExecutionInformation execucao) throws SQLException {
        String sql;
        Statement st = connection.createStatement();
        Statement st2 = connection.createStatement();
        java.sql.ResultSet rs;
        java.sql.ResultSet rs2;
        /*
         * Calcula rentabilidades e outros indicadores em períodos mensais
         */
        sql = "SELECT MAX(DT_COMPTC) AS UltimaDataComDados, COUNT(cnpj_fundo_id) AS numRegistros FROM doc_inf_diario_fundos WHERE cnpj_fundo_id=" + idFundo;
        rs = st.executeQuery(sql);
        if (rs.next()) {
            if (rs.getLong("numRegistros") == 0L) {
                // há um fundo cadastrado sem qualquer informação diária
                System.out.println("info: Fundo " + idFundo + " não contém nenhum registro diário.");
            } else {
                LocalDate ultimaData = rs.getDate("UltimaDataComDados").toLocalDate();
                String primeiroDiaDoUltimoMesCompleto = "0" + ultimaData.getMonthValue();
                primeiroDiaDoUltimoMesCompleto = ultimaData.getYear() + "-" + primeiroDiaDoUltimoMesCompleto.substring(primeiroDiaDoUltimoMesCompleto.length() - 2) + "-01";
                sql = "SELECT VL_QUOTA, DT_COMPTC, rentab_diaria FROM doc_inf_diario_fundos WHERE cnpj_fundo_id=" + idFundo + " AND DT_COMPTC<='" + primeiroDiaDoUltimoMesCompleto + "' ORDER BY DT_COMPTC DESC"; //calcula indicadores do último dia para trás, até o limite máximo de meses definido abaixo
                rs = st.executeQuery(sql);
                if (rs.next()) {
                    String dataFinal = rs.getDate("DT_COMPTC").toString();
                    // verifica se já existe
                    sql = "SELECT cnpj_fundo_id, tipo_benchmark_id, meses_acima_bench, sharpe, beta FROM indicadores_fundos WHERE cnpj_fundo_id=" + idFundo + " AND periodo_meses=1 AND data_final='" + dataFinal + "'";
                    rs2 = st2.executeQuery(sql);
                    if (!rs2.next()) { // se não há nada de um mês, não deve haver os demais também; então calcula tudo. Se há, não faz nada
                        // define um mercado para este fundo
                        int tipo_benchmark_id = mercado_do_fundo(connection, idFundo);

                        Month mesAnterior = rs.getDate("DT_COMPTC").toLocalDate().getMonth();
                        Month mesAtual;
                        double rentabilidade, sumRentabilidade = 0.0, minRentabilidade = 1e6, maxRentabilidade = -1e6;
                        List<Double> rentabilidades = new ArrayList<>();
                        List<Double> quotas = new LinkedList<>();
                        int numValores = 0, numMeses = 0;
                        do {
                            mesAtual = rs.getDate("DT_COMPTC").toLocalDate().getMonth();
                            if (mesAtual.equals(mesAnterior)) {
                                // acumula
                                numValores++;
                                quotas.add(0, rs.getDouble("VL_QUOTA")); // inclui em ordem cronológica para posterior cálculo do MaxDrawdown
                                rentabilidade = rs.getDouble("rentab_diaria");
                                rentabilidades.add(rentabilidade);
                                sumRentabilidade += rentabilidade;
                                if (rentabilidade < minRentabilidade) {
                                    minRentabilidade = rentabilidade;
                                }
                                if (rentabilidade > maxRentabilidade) {
                                    maxRentabilidade = rentabilidade;
                                }
                            } else {
                                // calcula indicadores do mês que terminou (e de todos os outros meses que já passou, de forma acumulada)
                                mesAnterior = mesAtual;
                                numMeses++;
                                double rentabilidadeMedia = sumRentabilidade / numValores;
                                double sumDesvioPadrao = 0.0;
                                for (Double rent : rentabilidades) {
                                    sumDesvioPadrao += Math.pow(rent - rentabilidadeMedia, 2);
                                }
                                double desvioPadrao;
                                if (numValores > 1) { // pois n-1 == 0
                                    desvioPadrao = Math.sqrt(sumDesvioPadrao / (numValores - 1));
                                } else {
                                    desvioPadrao = 0.0;
                                }
                                double maxQuota = Double.MIN_VALUE, drawDown, maxDrawdown = 0.0; // TODO: MaxQuota é muito negativa pois pode haver quotas negativas (check this out)
                                for (Double quota : quotas) {
                                    if (quota > maxQuota) {
                                        maxQuota = quota;
                                    } else {
                                        if (Math.abs(maxQuota) < 2e-7) { // maxQuota é zero (ou é muito próximo de zero)?
                                            drawDown = 0.0;
                                        } else {
                                            drawDown = quota / maxQuota - 1.0;
                                        }
                                        if (drawDown < maxDrawdown) {
                                            maxDrawdown = drawDown;
                                        }
                                    }
                                }
                                // Estes valores dependem do mercado (serão calculados depois)
                                String meses_acima_bench = null;
                                String sharpe = null;
                                String beta = null;
                                // salva no BD
                                sql = "INSERT INTO indicadores_fundos (cnpj_fundo_id, periodo_meses, data_final, rentabilidade, desvio_padrao, num_valores, rentab_min, rentab_max, max_drawdown, tipo_benchmark_id, meses_acima_bench, sharpe, beta) VALUES (" + idFundo + "," + numMeses + ",'" + dataFinal + "'," + rentabilidadeMedia + "," + desvioPadrao + "," + numValores + "," + minRentabilidade + "," + maxRentabilidade + "," + maxDrawdown + "," + tipo_benchmark_id + "," + meses_acima_bench + "," + sharpe + "," + beta + ")";
                                try {
                                    st2.execute(sql);
                                    execucao.incrementCountRecords();
                                    if (numMeses % 12 == 0) {
                                        if (!DEBUG) { connection.commit(); }
                                        long tempoAtual = System.currentTimeMillis();
                                        execucao.setTempoProcessamentoParte(tempoAtual - execucao.getFimProcessamento());
                                        execucao.setTempoProcessamentoTotal(tempoAtual - execucao.getInicioProcessamento());
                                        System.out.println("info: " + numMeses + " meses processados da última parte (" + execucao.getTempoProcessamentoParte() + " ms)");//, sendo " + countRecordsComErro + " com erro e " + countRecordsRepetidos + " preexistentes");
                                        System.out.println("info: " + numMeses + " meses processados até o momento (totalizando " + execucao.getTempoProcessamentoTotal().intValue() + " ms)");//, sendo " + countRecordsComErro + " com erro e " + countRecordsRepetidos + " preexistentes");
                                        execucao.setFimProcessamento(tempoAtual);
                                    }
                                } catch (SQLException ex) {
                                    // Logger.getLogger(CVMDatabaseThread.class.getName()).log(Level.SEVERE, null, ex);
                                    execucao.incrementCountRecordsRepetido();
                                }
                            }
                        } while (rs.next() && numMeses <= 48);
                        if (!DEBUG) { connection.commit(); }
                    }
                }
            }
        }
    }

    /**
     * Encontra todos os fundos desatualizado, ou seja, que possui rentabilidades diárias
     * não computadas. O objetivo é ter certeiza que o mercado, neste período, está
     * atualizado e não será modificado depois do cálculo de algum indice (Sharpe ou Beta).
     * @return Lista com ID de fundos sem indicadores de mercado calculados
     * @throws SQLException quando algum dado necessário não pode ser acessado pelo banco
     */
    private List<Integer> calcula_mercado(Connection connection, ExecutionInformation execucao) throws SQLException {
        // TODO
        String sql = "SELECT DISTINCT cnpj_fundo_id FROM doc_inf_diario_fundos WHERE rentabilidade IS NULL";
        Statement st = connection.createStatement();
        java.sql.ResultSet rs = st.executeQuery(sql);
        List<Integer> fundosSemIndicadoresDeMercado = new ArrayList<>();
        while (rs.next()) {
            int idFundo = rs.getInt("cnpj_fundo_id");
            calcula_rentabilidade_diaria(connection, idFundo, execucao);
            calcula_indicadores(connection, idFundo, execucao);
            fundosSemIndicadoresDeMercado.add(idFundo);
        }
        return fundosSemIndicadoresDeMercado;
    }

    /**
     * Busca o mercado que esse fundo pertence, caso ele não pertença a nenhum, será
     * atribuído a um mercado de maneira aleatoria.
     * @param connection conexão com o banco
     * @param idFundo ID do fundo
     * @return Mercado que este fundo pertence
     * @throws SQLException Quando não for possível acessar o banco
     */
    private int mercado_do_fundo(Connection connection, int idFundo) throws SQLException {
        // TODO
        String sql = "SELECT tipo_benchmark_id FROM indicadores_fundos WHERE cnpj_fundo_id=" + idFundo + " AND tipo_benchmark_id IS NOT NULL";
        Statement st = connection.createStatement();
        java.sql.ResultSet rs = st.executeQuery(sql);
        if (rs.next()) {
            return rs.getInt("tipo_benchmark_id");
        }
        Random random = new Random();
        return random.nextInt(5) + 1;
    }

    /**
     * Busca a rentabilidade diária de um fundo em uma data.
     * @param idFundo ID do fundo
     * @param data Data da rentabilidade
     * @return Rentabilidade diária do fundo
     * @throws SQLException Quando não foi possível encontrar a rentabilidade diária no banco
     */
    private BigDecimal rentabilidade_daria_fundo(Connection connection, int idFundo, String data) throws SQLException {
        final String IMPOSSIVEL_DE_BUSCAR_RENTABILIDADE = "Não foi possível buscar a rentabilidade diária";
        // TODO
        String sql = "SELECT rentab_diaria FROM doc_inf_diario_fundos WHERE DT_COMPTC='" + data + "' AND cnpj_fundo_id=" + idFundo + " AND rentab_diaria IS NOT NULL";
        Statement st = connection.createStatement();
        java.sql.ResultSet rs = st.executeQuery(sql);
        if (rs.next()) {
            return rs.getBigDecimal("rentab_diaria");
        }
        throw new SQLException(IMPOSSIVEL_DE_BUSCAR_RENTABILIDADE);
    }

    /**
     * Busca uma lista de rentabilidades diárias de um fundo em um intervalo de tempo.
     * @param idFundo ID do fundo
     * @param data Data final do intervalo da rentabilidade
     * @param numValores número de valores (dias) no intervalo
     * @return Lista ordenada de rentabilidades diária do fundo
     * @throws SQLException Quando não foi possível encontrar alguma rentabilidade diária no banco
     */
    protected List<BigDecimal> rentabilidade_daria_fundo(Connection connection, int idFundo, String data, int numValores) throws SQLException {
        final String IMPOSSIVEL_DE_CALCULAR_RENTABILIDADES = "Não foi possível calcular as rentabilidades diárias";
        List<BigDecimal> rentabilidadesDiarias = new ArrayList<>();
        GregorianCalendar calendar = new GregorianCalendar();
        try {
            calendar.setTime(formatadorData.parse(data));
            calendar.add(Calendar.DAY_OF_MONTH, -numValores); // vai para o início do intervalo
            for (int i = 0; i < numValores; i++) {
                calendar.add(Calendar.DAY_OF_MONTH, 1); // Avança 1 dia
                BigDecimal rentabilidadeDiaria = rentabilidade_daria_fundo(connection, idFundo, formatadorData.format(calendar));
                rentabilidadesDiarias.add(rentabilidadeDiaria);
            }
            return rentabilidadesDiarias;
        } catch (ParseException ignore) { }
        throw new SQLException(IMPOSSIVEL_DE_CALCULAR_RENTABILIDADES);
    }

    /**
     * Busca o desvio padrão de um fundo neste intervalo de tempo.
     * @param idFundo ID do fundo
     * @param dataFinal Data final do intervalo a ser calculado o desvio padrão
     * @param numValores Número de entradas (dias) pertencentes ao intervalo
     * @return desvio padrão do mercado no intervalo
     * @throws SQLException quando algum dado necessário não pode ser acessado pelo banco
     */
    protected BigDecimal desvio_padrao_fundo(Connection connection, int idFundo, String dataFinal, int numValores) throws SQLException {
        BigDecimal rentabilidadeMedia = rentabilidade_media_fundo(connection, idFundo, dataFinal, numValores);
        List<BigDecimal> rentabilidadesDiarias = rentabilidade_daria_fundo(connection, idFundo, dataFinal, numValores);
        BigDecimal desvioPadrao = new BigDecimal(0);
        for (BigDecimal rentabilidadeDiaria : rentabilidadesDiarias) {
            desvioPadrao = desvioPadrao.add(rentabilidadeDiaria.subtract(rentabilidadeMedia).pow(2, PRECISAO_DECIMAL));
        }
        desvioPadrao = desvioPadrao.divide(new BigDecimal(numValores), PRECISAO_DECIMAL);
        desvioPadrao = desvioPadrao.sqrt(PRECISAO_DECIMAL);
        return desvioPadrao;
    }

    /**
     * Busca a rentabilidade média do fundo no intervalode tempo.
     * @param idFundo ID do fundo
     * @param dataFinal data final do intervalo de tempo
     * @param numValores Número de valores (dias) pertencentes ao intervalo
     * @return Rentabilidade média
     * @throws SQLException Quando não foi possível encontrar alguma rentabilidade diária do intervalo no banco
     */
    protected BigDecimal rentabilidade_media_fundo(Connection connection, int idFundo, String dataFinal, int numValores) throws SQLException {
        List<BigDecimal> rentabilidadesDiarias = rentabilidade_daria_fundo(connection, idFundo, dataFinal, numValores);
        BigDecimal rentabilidadeSoma = new BigDecimal(0);
        for (BigDecimal rentabilidadeDiaria : rentabilidadesDiarias) {
            rentabilidadeSoma = rentabilidadeSoma.add(rentabilidadeDiaria);
        }
        return rentabilidadeSoma.divide(new BigDecimal(numValores), PRECISAO_DECIMAL);
    }

    /**
     * Busca a rentabilidade média do mercado no intervalode tempo. Utiliza uma cache para evitar o cálculo
     * de valores já calculados.
     * @param tipoBenchmarkId ID do benchmark (mercado)
     * @param dataFinal Data final do intervalo de tempo considerado
     * @param numValores Número de valores (dias) pertencentes ao intervalo de tempo
     * @return rentabilidade média do mercado
     * @throws SQLException quando algum dado necessário não pode ser acessado pelo banco
     */
    protected BigDecimal rentabilidade_media_mercado(Connection connection, int tipoBenchmarkId, String dataFinal, int numValores) throws SQLException {
        Pair<String, Integer> cacheEntry = new Pair<>(dataFinal, numValores);
        Map<Pair<String, Integer>, BigDecimal> cache = this.rentabilidadeMediaMercadoCache.get(tipoBenchmarkId);
        if (cache.containsKey(cacheEntry)) { // Já foi calculado?
            return cache.get(cacheEntry);
        }
        List<BigDecimal> rentabilidadesDiarias = rentabilidade_diaria_mercado(connection, tipoBenchmarkId, dataFinal, numValores);
        BigDecimal rentabilidadeSoma = BigDecimal.ZERO;
        for (BigDecimal rentabilidadeDiaria : rentabilidadesDiarias) {
            rentabilidadeSoma = rentabilidadeSoma.add(rentabilidadeDiaria);
        }
        BigDecimal rentabilidadeMedia = rentabilidadeSoma.divide(new BigDecimal(numValores), PRECISAO_DECIMAL);
        cache.put(cacheEntry, rentabilidadeMedia);
        return rentabilidadeMedia;
    }

    /**
     * Busca uma lista ordenada de rentabilidades diárias do mercado em um intervalo de tempo. Utiliza uma
     * cache para evitar o cálculo de valores já calculados.
     * @param tipoBenchmarkId ID do benchmark (mercado)
     * @param dataFinal Data final do intervalo de tempo
     * @param numValores número de valores (dias) pertencentes ao intervalo de tempo
     * @return Lista de rentabilidade diárias ordenada por tempo
     * @throws SQLException quando algum dado necessário não pode ser acessado pelo banco
     */
    protected List<BigDecimal> rentabilidade_diaria_mercado(Connection connection, int tipoBenchmarkId, String dataFinal, int numValores) throws SQLException {
        final String IMPOSSIVEL_DE_CALCULAR_RENTABILIDADES = "Não foi possível calcular as rentabilidades diárias";
        List<BigDecimal> rentabilidadesDiaria = new ArrayList<>();
        GregorianCalendar calendar = new GregorianCalendar();
        try {
            calendar.setTime(formatadorData.parse(dataFinal));
            calendar.add(Calendar.DAY_OF_MONTH, -numValores); // vai para o início do intervalo
            for (int i = 0; i < numValores; i++) {
                calendar.add(Calendar.DAY_OF_MONTH, 1); // Avança 1 dia
                BigDecimal rentabilidadeDiaria = rentabilidade_diaria_mercado(connection, tipoBenchmarkId, formatadorData.format(calendar));
                rentabilidadesDiaria.add(rentabilidadeDiaria);
            }
            return rentabilidadesDiaria;
        } catch (ParseException ignore) { }
        throw new SQLException(IMPOSSIVEL_DE_CALCULAR_RENTABILIDADES);
    }

    /**
     * Busca a volatilidade do mercado neste intervalo de tempo. Utiliza uma cache para evitar o cálculo de
     * valores já calculados.
     * @param tipoBenchmarkId ID do benchmark (mercado)
     * @param dataFinal Data final do intervalo a ser calculado o desvio padrão
     * @param numValores Número de entradas (dias) pertencentes ao intervalo
     * @return desvio padrão do mercado no intervalo
     * @throws SQLException quando algum dado necessário não pode ser acessado pelo banco
     */
    protected BigDecimal volatilidade_mercado(Connection connection, int tipoBenchmarkId, String dataFinal, int numValores) throws SQLException {
        Pair<String, Integer> cacheEntry = new Pair<>(dataFinal, numValores);
        Map<Pair<String, Integer>, BigDecimal> cache = this.desvioPadraoMercadoCache.get(tipoBenchmarkId);
        if (cache.containsKey(cacheEntry)) {
            return cache.get(cacheEntry);
        }
        // busca no banco de dados todos os fundos deste mercado
        // TODO
        String sql = "SELECT DISTINCT(cnpj_fundo_id) FROM doc_inf_diario_fundos WHERE DT_COMPTC=" + dataFinal + " and cnpj_fundo_id IN" +
                "(SELECT cnpj_fundo_id FROM indicadores_fundos WHERE tipo_benchmark_id=" + tipoBenchmarkId + " )";
        Statement st = connection.createStatement();
        java.sql.ResultSet rs = st.executeQuery(sql);
        BigDecimal volatilidade = BigDecimal.ZERO;
        while (rs.next()) {
            // Soma seus desvios padrões
            int idFundo = rs.getInt("cnpj_fundo_id");
            BigDecimal devPadFundo = desvio_padrao_fundo(connection, idFundo, dataFinal, numValores);
            volatilidade = volatilidade.add(devPadFundo);
        }
        volatilidade = volatilidade.divide(new BigDecimal(numValores), PRECISAO_DECIMAL);
        cache.put(cacheEntry, volatilidade);
        return volatilidade;
    }

    /**
     * Busca a rentabilidade diária do mercado na data. Utiliza uma cache para evitar o cálculo
     * de valores já calculados.
     * @param tipoBenchmarkId ID do benchmark (mercado)
     * @param data Data em que deve-se buscar a rentabilidade
     * @return Rentabilidade diária
     * @throws SQLException quando algum dado necessário não pode ser acessado pelo banco
     */
    private BigDecimal rentabilidade_diaria_mercado(Connection connection, int tipoBenchmarkId, String data) throws SQLException {
        final String IMPOSSIVEL_DE_CALCULAR_RENTABILIDADE = "Não foi possível calcular a rentabilidade diária do mercado";
        Map<String, BigDecimal> cache = this.rentabilidadeDiariaMercadoCache.get(tipoBenchmarkId);
        if (cache.containsKey(data)) {
            return cache.get(data);
        }
        // busca no banco de dados a média das rentabilidade diárias dos fundos pertencetes ao mercado
        // TODO
        String sql = "SELECT AVG(rentab_diaria) AS rentabilidade_benchmark FROM doc_inf_diario_fundos WHERE DT_COMPTC=" + data + " and cnpj_fundo_id IN" +
                "(SELECT cnpj_fundo_id FROM indicadores_fundos WHERE tipo_benchmark_id=" + tipoBenchmarkId + " )";
        Statement st = connection.createStatement();
        java.sql.ResultSet rs = st.executeQuery(sql);
        if (!rs.next()) {
            throw new SQLException(IMPOSSIVEL_DE_CALCULAR_RENTABILIDADE);
        }
        BigDecimal rentabilidadeDiaria = rs.getBigDecimal("rentabilidade_benchmark");
        cache.put(data, rentabilidadeDiaria);
        return rentabilidadeDiaria;
    }

    /**
     * Cálcula o indicador generalizado de Sharpe para um fundo e seu mercado em um intervalo de tempo.
     * Considera que a rentabilidade e volatilidade de um fundo já foi calculada.
     * @param rentabilidadeMediaFundo Renabilidade média do fundo no intervalo de tempo
     * @param volatilidadeFundo Volatilidade do fundo no intervalo de tempo
     * @param tipoBenchmarkId ID do benchmark (mercado)
     * @param dataFinal Data final do intervalo
     * @param numValores Número de valores (dias) no intervalo
     * @return Indicador sharpe generalizado
     * @throws SQLException quando não for possível acessar algum valor no banco
     */
    private BigDecimal sharpe_generalizado_fundo(Connection connection, BigDecimal rentabilidadeMediaFundo, BigDecimal volatilidadeFundo, int tipoBenchmarkId, String dataFinal, int numValores) throws SQLException {
        BigDecimal rentabilidadeMediaMercado = rentabilidade_media_mercado(connection, tipoBenchmarkId, dataFinal, numValores);
        BigDecimal volatilidadeMercado = volatilidade_mercado(connection, tipoBenchmarkId, dataFinal, numValores);
        return rentabilidadeMediaFundo.subtract(rentabilidadeMediaMercado)
                .divide(volatilidadeFundo.subtract(volatilidadeMercado), PRECISAO_DECIMAL);
    }

    /**
     * Cálcula o indicador generalizado de Sharpe para um fundo e seu mercado em um intervalo de tempo.
     * @param idFundo ID do fundo
     * @param tipoBenchmarkId ID do benchmark (mercado)
     * @param dataFinal Data final do intervalo
     * @param numValores Número de valores (dias) no intervalo
     * @return Indicador sharpe generalizado
     * @throws SQLException quando não for possível acessar algum valor no banco
     */
    protected BigDecimal sharpe_generalizado_fundo(Connection connection, int idFundo, int tipoBenchmarkId, String dataFinal, int numValores) throws SQLException {
        BigDecimal rentabilidadeMediaFundo = rentabilidade_media_fundo(connection, idFundo, dataFinal, numValores);
        BigDecimal volatilidadeFundo = desvio_padrao_fundo(connection, idFundo, dataFinal, numValores);
        return sharpe_generalizado_fundo(connection, rentabilidadeMediaFundo, volatilidadeFundo, tipoBenchmarkId, dataFinal, numValores);
    }

    /**
     * Cálcula o indicador de Sharpe para um fundo e um mercado, conforme um intervalo de
     * tempo.
     * Considera que a rentabilidade e volatilidade de um fundo já foi calculada.
     * @param rentabilidadeMediaFundo Rentabilidade média do fundo no intervalo de tempo
     * @param volatilidadeFundo Volatilidade do fundo no intervalo de tempo
     * @param tipoBenchmarkId ID do benchmark (mercado)
     * @param dataFinal Data final do intervalo
     * @param numValores Número de valores (dias) no intervalo
     * @return Indicador sharpe
     * @throws SQLException quando não for possível acessar algum valor no banco
     */
    private BigDecimal sharpe_fundo(Connection connection, BigDecimal rentabilidadeMediaFundo, BigDecimal volatilidadeFundo, int tipoBenchmarkId, String dataFinal, int numValores) throws SQLException {
        BigDecimal rentabilidadeMediaMercado = rentabilidade_media_mercado(connection, tipoBenchmarkId, dataFinal, numValores);
        return rentabilidadeMediaFundo.subtract(rentabilidadeMediaMercado)
                .divide(volatilidadeFundo, PRECISAO_DECIMAL);
    }

    /**
     * Cálcula o indicador de Sharpe para um fundo e um mercado, conforme um intervalo de
     * tempo.
     * @param idFundo ID do fundo
     * @param tipoBenchmarkId ID do benchmark (mercado)
     * @param dataFinal Data final do intervalo
     * @param numValores Número de valores (dias) no intervalo
     * @return Indicador sharpe
     * @throws SQLException quando não for possível acessar algum valor no banco
     */
    protected BigDecimal sharpe_fundo(Connection connection, int idFundo, int tipoBenchmarkId, String dataFinal, int numValores) throws SQLException {
        BigDecimal rentabilidadeMediaFundo = rentabilidade_media_fundo(connection, idFundo, dataFinal, numValores);
        BigDecimal volatilidadeFundo = desvio_padrao_fundo(connection, idFundo, dataFinal, numValores);
        return sharpe_fundo(connection, rentabilidadeMediaFundo, volatilidadeFundo, tipoBenchmarkId, dataFinal, numValores);
    }

    /**
     * Cálcula o indicador de Beta para um fundo e um mercado, conforme um intervalo de
     * tempo.
     * Considera que a rentabilidade média do fundo já foi calculada
     * @param idFundo ID do fundo
     * @param tipoBenchmarkId ID do benchmark (mercado)
     * @param dataFinal Data final do intervalo
     * @param numValores Número de valores (dias) no intervalo
     * @return Indicador beta
     * @throws SQLException quando não for possível acessar algum valor no banco
     */
    private BigDecimal beta_fundo(Connection connection, BigDecimal rentabilidadeMediaFundo, int idFundo, int tipoBenchmarkId, String dataFinal, int numValores) throws SQLException {
        List<BigDecimal> rentabilidadesDiariasFundo = rentabilidade_daria_fundo(connection, idFundo, dataFinal, numValores);
        List<BigDecimal> rentabilidadeDiariasMercado = rentabilidade_diaria_mercado(connection, idFundo, dataFinal, numValores);
        BigDecimal rentabilidadeMediaMercado = rentabilidade_media_mercado(connection, tipoBenchmarkId, dataFinal, numValores);
        BigDecimal volatibilidadeMercado = volatilidade_mercado(connection, tipoBenchmarkId, dataFinal, numValores);
        // convolução
        BigDecimal conv = new BigDecimal(0);
        for (int i = 0; i < numValores; i++) {
            BigDecimal rentFundo = rentabilidadesDiariasFundo.get(i);
            BigDecimal rentMerc = rentabilidadeDiariasMercado.get(i);
            conv = conv.add(
                    rentFundo.subtract(rentabilidadeMediaFundo).multiply(rentMerc.subtract(rentabilidadeMediaMercado)));
        }
        conv = conv.divide(new BigDecimal(numValores), PRECISAO_DECIMAL);
        // beta
        return conv.divide(volatibilidadeMercado, PRECISAO_DECIMAL);
    }

    /**
     * Cálcula o indicador de Beta para um fundo e um mercado, conforme um intervalo de
     * tempo.
     * @param idFundo ID do fundo
     * @param tipoBenchmarkId ID do benchmark (mercado)
     * @param dataFinal Data final do intervalo
     * @param numValores Número de valores (dias) no intervalo
     * @return Indicador beta
     * @throws SQLException quando não for possível acessar algum valor no banco
     */
    protected BigDecimal beta_fundo(Connection connection, int idFundo, int tipoBenchmarkId, String dataFinal, int numValores) throws SQLException {
        BigDecimal rentabilidadeMediaFundo = rentabilidade_media_fundo(connection, idFundo, dataFinal, numValores);
        return beta_fundo(connection, rentabilidadeMediaFundo, idFundo, tipoBenchmarkId, dataFinal, numValores);
    }

    /**
     * Calcula os indicadores de Sharde de um fundo. Os Resultados são armazenados na
     * tabela 'indicadores_fundos'. O indicador calculado é referentes ao mercado que
     * o fundo pertence.
     * @param connection Conexão com o banco de dados
     * @param idFundo ID do fundo
     * @param execucao Gerador de estatística da execução
     * @throws SQLException Quando não é possível acessar o banco
     */
    private void calcula_indice_sharp(Connection connection, int idFundo, ExecutionInformation execucao) throws SQLException {
        // Seleciona os indices do fundo onde sharpe não foi calculado
        // TODO
        String sql = "SELECT periodo_meses, num_valores, data_final, rentabilidade, desvio_padrao, tipo_benchmark_id FROM indicadores_fundos" +
                " WHERE cnpj_fundo_id=" + idFundo +
                " AND sharpe IS NULL" +
                " AND num_valores IS NOT NULL" +
                " AND tipoBenchmarkId IS NOT NULL";
        Statement st = connection.createStatement();
        Statement st2 = connection.createStatement();
        java.sql.ResultSet rs = st.executeQuery(sql);
        while (rs.next()) {
            try {
                int numMeses = rs.getInt("periodo_meses");
                int numValores = rs.getInt("num_valores");
                String dataFinal = rs.getDate("data_final").toString();
                BigDecimal rentabilidade = rs.getBigDecimal("rentabilidade");
                BigDecimal desvioPadrao = rs.getBigDecimal("desvio_padrao");
                int tipoBenchmarkId = rs.getInt("tipo_benchmark_id");

                // Chaves primária é (cnpj_fundo_id, periodo_meses, data_final)
                BigDecimal sharpe = sharpe_fundo(connection, rentabilidade, desvioPadrao, tipoBenchmarkId, dataFinal, numValores);
                sql = "UPDATE indicadores_fundos SET sharpe=" + sharpe + " WHERE cnpj_fundo_id=" + idFundo + " and periodo_meses=" + numMeses + " and data_final=" + dataFinal;
                st2.execute(sql);

                long tempoAtual = System.currentTimeMillis();
                execucao.setTempoProcessamentoParte(tempoAtual - execucao.getFimProcessamento());
                execucao.setTempoProcessamentoTotal(tempoAtual - execucao.getInicioProcessamento());
                System.out.println("info: (" + idFundo + ", " + numMeses + ", " + dataFinal + ") processado o índice de beta (" + execucao.getTempoProcessamentoParte() + " ms)");//, sendo " + countRecordsComErro + " com erro e " + countRecordsRepetidos + " preexistentes");
                execucao.setFimProcessamento(tempoAtual);
            } catch (SQLException e) {
                execucao.incrementCountRecordsComErro();
            }
        }
        if (!DEBUG) { connection.commit(); }
    }

    /**
     * Calcula os indicadores Beta de um fundo. Os Resultados são armazenados na
     * tabela 'indicadores_fundos'. O indicador calculado é referentes ao mercado que
     * o fundo pertence.
     * @param connection Conexão com o banco de dados
     * @param idFundo ID do fundo
     * @param execucao Gerador de estatística da execução
     * @throws SQLException Quando não é possível acessar o banco
     */
    private void calcula_indice_beta(Connection connection, int idFundo, ExecutionInformation execucao) throws SQLException {
        // Seleciona os indices do fundo onde beta não foi calculado
        // TODO
        String sql = "SELECT periodo_meses, num_valores, data_final, rentabilidade, tipo_benchmark_id FROM indicadores_fundos" +
                " WHERE cnpj_fundo_id=" + idFundo +
                " AND beta IS NULL" +
                " AND num_valores IS NOT NULL" +
                " AND tipo_benchmark_id IS NOT NULL";
        Statement st = connection.createStatement();
        Statement st2 = connection.createStatement();
        java.sql.ResultSet rs = st.executeQuery(sql);
        while (rs.next()) {
            try {
                int numMeses = rs.getInt("periodo_meses");
                int numValores = rs.getInt("num_valores");
                String dataFinal = rs.getDate("data_final").toString();
                BigDecimal rentabilidade = rs.getBigDecimal("rentabilidade");
                int tipoBenchmarkId = rs.getInt("tipo_benchmark_id");

                // Chaves primária é (cnpj_fundo_id, periodo_meses, data_final)
                BigDecimal beta = beta_fundo(connection, rentabilidade, idFundo, tipoBenchmarkId, dataFinal, numValores);
                sql = "UPDATE indicadores_fundos SET beta=" + beta + " WHERE cnpj_fundo_id=" + idFundo + " AND periodo_meses=" + numMeses + " AND data_final=" + dataFinal;
                st2.execute(sql);

                long tempoAtual = System.currentTimeMillis();
                execucao.setTempoProcessamentoParte(tempoAtual - execucao.getFimProcessamento());
                execucao.setTempoProcessamentoTotal(tempoAtual - execucao.getInicioProcessamento());
                System.out.println("info: (" + idFundo + ", " + numMeses + ", " + dataFinal + ") processado o índice de beta (" + execucao.getTempoProcessamentoParte() + " ms)");//, sendo " + countRecordsComErro + " com erro e " + countRecordsRepetidos + " preexistentes");
                execucao.setFimProcessamento(tempoAtual);
            } catch (SQLException e) {
                execucao.incrementCountRecordsComErro();
            }
        }
        if (!DEBUG) { connection.commit(); }
    }

    /**
     * Atualiza o banco de dados com indicadores.
     * @return Array com estatísticas de execução
     */
    public Object[] calcula_indicadores_DOC_INF_DIARIO() {
        return this.calcula_indicadores_DOC_INF_DIARIO(this.connection);
    }

    /**
     * Atualiza o banco de dados com indicadores.
     * @param connection Conexão com o banco de dados
     * @return Array com estatísticas de execução
     */
    private Object[] calcula_indicadores_DOC_INF_DIARIO(Connection connection) {
        ExecutionInformation execucao = new ExecutionInformation();
        try {
            int idFundo = Integer.parseInt(this.datafilename);
            calcula_rentabilidade_diaria(connection, idFundo, execucao);
            calcula_indicadores(connection, idFundo, execucao);
            List<Integer> fundosSemIndicadoresDeMercado = calcula_mercado(connection, execucao);
            calcula_indice_sharp(connection, idFundo, execucao);
            calcula_indice_beta(connection, idFundo, execucao);
            if (ATUALIZACAO == TipoAtualizacao.COMPLETA) {
                for (Integer idFundo_ : fundosSemIndicadoresDeMercado) {
                    calcula_indice_sharp(connection, idFundo_, execucao);
                    calcula_indice_beta(connection, idFundo_, execucao);
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(InvestFundsManager.class.getName()).log(Level.SEVERE, null, ex);
            execucao.incrementCountRecordsComErro();
        }
        return execucao.getRecordsArray();
    }

    /**
     * Classe que armazena dois dados em um par. Utilizado quando a chave de acesso à cache depende de múltiplos valores.
     * @param <U> Classe do primeiro dado pertencente a chave.
     * @param <T> Classe do segundo dado pertencente a chave.
     */
    private static class Pair<U extends Comparable<U>, T extends Comparable<T>> implements Comparable<Pair<U, T>> {
        private final U first;
        private final T second;

        public Pair(U first, T second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public int compareTo(Pair<U, T> p) {
            U f1 = this.getFirst();
            U f2 = p.getFirst();
            T s1 = this.getSecond();
            T s2 = p.getSecond();
            if (f1 == null && f2 == null) {
                if (s1 == null && s2 == null) {
                    return 0;
                } else if (s1 == null) {
                    return -1;
                } else if (s2 == null) {
                    return 1;
                }
                return s1.compareTo(s2);
            } else if (f1 == null) {
                return -1;
            } else if (f2 == null) {
                return 1;
            }
            int p1 = f1.compareTo(f2);
            if (p1 != 0) {
                return p1;
            } else {
                if (s1 == null && s2 == null) {
                    return 0;
                } else if (s1 == null) {
                    return -1;
                } else if (s2 == null) {
                    return 1;
                }
                return s1.compareTo(s2);
            }
        }

        private U getFirst() {
            return this.first;
        }

        private T getSecond() {
            return this.second;
        }
    }

    /**
     * Indica que, se atualizar o mercado, deve-se calcular todos os indicadores de
     * mercado dos fundos atualizados. Completa atualizará todos, Parcial atualizará
     * somente o fundo de interesse.
     */
    private enum TipoAtualizacao {
        COMPLETA,
        PARCIAL
    }
}