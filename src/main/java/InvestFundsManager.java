import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InvestFundsManager {
    private final String databaseName;
    private final String databaseUsername;
    private final String databasePassword;
    private final String JDBC_DRIVER;
    private final String DB_URL;
    private final String DB_PROPERTIES;
    private Connection connection;
    private final String datafilename;

    public InvestFundsManager() throws ClassNotFoundException, SQLException {
        databaseName = "investFunds";
        databaseUsername = "<username>";
        databasePassword = "<password>";
        JDBC_DRIVER = "com.mysql.jdbc.Driver";
        DB_PROPERTIES = "?useSSL=false";
        DB_URL = "jdbc:mysql://localhost:3306/";
        datafilename = "00";
        // ...
        ConnectToDB();
        // calcula_indicadores_DOC_INF_DIARIO(connection);
    }

    private void ConnectToDB() throws ClassNotFoundException, SQLException {
        Class.forName(this.JDBC_DRIVER);
        connection = DriverManager.getConnection(this.DB_URL + this.databaseName + this.DB_PROPERTIES, this.databaseUsername, this.databasePassword);
        connection.setAutoCommit(false);
    }


    private Object[] calcula_indicadores_DOC_INF_DIARIO(Connection connection) {
        long inicioProcessamento = System.currentTimeMillis();
        long tempoProcessamentoParte, tempoProcessamentoTotal, fimProcessamento = inicioProcessamento;
        long countRecords = 0L;
        long countRecordsComErro = 0L;
        long countRecordsRepetidos = 0L;
        try {
            int idFundo = Integer.parseInt(this.datafilename);
            String sql;
            Statement st = connection.createStatement();
            Statement st2 = connection.createStatement();
            java.sql.ResultSet rs;
            java.sql.ResultSet rs2;

            sql = "select VL_QUOTA, DT_COMPTC from doc_inf_diario_fundos where cnpj_fundo_id=" + idFundo + " and (volat_diaria is null) order by DT_COMPTC ASC"; // registros sem rentabilidade, para calcular
            rs = st.executeQuery(sql);
            if (rs.next()) {
                Double valorQuotaAnterior, rentabAcumulAnterior, valorQuotaAtual, rentabilidade, volatilidade, rentabilidadeAcumulada, maxValorQuota, drawdown, temp;//, somaRent;
                Double rent1 = Double.NaN, rent2 = Double.NaN, rent3 = Double.NaN;
                String dataAtual;
                String firstEmptyDate = rs.getDate("DT_COMPTC").toString(); // primeira data sem rentabilidade calculada. Tenta buscar a última rentabilidade calculada antes dessa
                sql = "select VL_QUOTA, rentab_acumulada from doc_inf_diario_fundos where cnpj_fundo_id=" + idFundo + " and (volat_diaria is not null) and DT_COMPTC<'" + firstEmptyDate + "' order by DT_COMPTC DESC";
                rs2 = st2.executeQuery(sql);
                if (rs2.next()) { // há valores anteriores à faixa de valores vazios. Pega o último valor de cota conhecido antes disso
                    valorQuotaAnterior = rs2.getDouble("VL_QUOTA");
                    rentabAcumulAnterior = rs2.getDouble("rentab_acumulada");
                    sql = "select Max(VL_QUOTA) as MAX_VL_QUOTA from doc_inf_diario_fundos where cnpj_fundo_id=" + idFundo + " and DT_COMPTC<'" + firstEmptyDate + "' order by DT_COMPTC DESC";
                    rs2 = st2.executeQuery(sql);
                    if (rs2.next()) {
                        maxValorQuota = rs2.getDouble("MAX_VL_QUOTA");
                    } else {
                        // nuncA deveria acontecer
                        maxValorQuota = 0.0; //?????
                    }
                } else {
                    // não há valores anteriores. Então o valor anterior deve ser o primeiro valor da quota no período sem valores calculados
                    valorQuotaAnterior = rs.getDouble("VL_QUOTA");
                    rentabAcumulAnterior = 0.0;
                    maxValorQuota = valorQuotaAnterior;
                }
                // tendo um valor anterior, varre todo o período calculando a rentabilidade
                do {
                    // calcula rentabilidades
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
                    if (valorQuotaAtual > maxValorQuota) {
                        maxValorQuota = valorQuotaAtual;
                    }
                    if (valorQuotaAtual < maxValorQuota) {
                        drawdown = valorQuotaAtual / maxValorQuota - 1.0;
                    } else {
                        drawdown = 0.0;
                    }
                    // salva no BD
                    sql = "update doc_inf_diario_fundos set rentab_diaria=" + rentabilidade + ", rentab_acumulada=" + rentabilidadeAcumulada + ", volat_diaria=" + volatilidade + ", drawdown=" + drawdown + " where cnpj_fundo_id=" + idFundo + " and DT_COMPTC='" + dataAtual + "'";
                    st2.execute(sql);
                    // TODO check por substituir as linhas anteriores por
                    //rs.updateDouble("rentab_diaria", rentabilidade);
                    //
                    countRecords++;
                    if (countRecords % 1e2 == 0) {
                        connection.commit();
                        if (countRecords % 1e3 == 0) {
                            tempoProcessamentoParte = (System.currentTimeMillis() - fimProcessamento);
                            tempoProcessamentoTotal = (System.currentTimeMillis() - inicioProcessamento);
                            System.out.println("info: " + countRecords / 1e3 + "K registros processados até o momento (totalizando " + (int) (tempoProcessamentoTotal) + " ms)");//, sendo " + countRecordsComErro + " com erro e " + countRecordsRepetidos + " preexistentes");
                            fimProcessamento = System.currentTimeMillis();
                        }
                    }
                    // atualiza vars
                    valorQuotaAnterior = valorQuotaAtual;
                    rentabAcumulAnterior = rentabilidadeAcumulada;
                } while (rs.next());
                connection.commit();
            } else {
                // não há valores vazios a serem calculados nos indicadores diários. Estão todos calculados e prontos. Não há o que fazer (ou simplesmente não há qualquer registro nos informativos diários
                countRecordsRepetidos++;
            }
            //
            // Calcula rentabilidades e outros indicadores em períodos mensais
            //
            sql = "select Max(DT_COMPTC) as UltimaDataComDados, Count(cnpj_fundo_id) as numRegistros from doc_inf_diario_fundos where cnpj_fundo_id=" + idFundo;
            rs = st.executeQuery(sql);
            if (rs.next()) {
                if (rs.getLong("numRegistros") == 0L) {
                    // há um fundo cadastrado sem qualquer informação diária
                    // TODO: Registrar isso!!!
                } else {
                    LocalDate ultimaData = rs.getDate("UltimaDataComDados").toLocalDate();
                    String primeiroDiaAposUltimoMesCompleto = "0" + String.valueOf(ultimaData.getMonthValue());
                    primeiroDiaAposUltimoMesCompleto = String.valueOf(ultimaData.getYear()) + "-" + primeiroDiaAposUltimoMesCompleto.substring(primeiroDiaAposUltimoMesCompleto.length() - 2) + "-01";
                    sql = "select VL_QUOTA, DT_COMPTC, rentab_diaria from doc_inf_diario_fundos where cnpj_fundo_id=" + idFundo + " and DT_COMPTC<'" + primeiroDiaAposUltimoMesCompleto + "' order by DT_COMPTC DESC"; //calcula indicadores do último dia para trás, até o limite máximo de meses definido abaixo
                    rs = st.executeQuery(sql);
                    if (rs.next()) {
                        String dataFinal = rs.getDate("DT_COMPTC").toString();
                        // verifica se já existe
                        sql = "select cnpj_fundo_id from indicadores_fundos where cnpj_fundo_id=" + idFundo + " and periodo_meses=1 and data_final='" + dataFinal + "'";
                        rs2 = st2.executeQuery(sql);
                        if (!rs2.next()) { // se não há nada de um mês, não deve haver os demais também; então calcula tudo. Se há, não faz nada
                            Month mesAnterior = rs.getDate("DT_COMPTC").toLocalDate().getMonth();
                            Month mesAtual;
                            double rentabilidade, sumRentabilidade = 0.0, minRentabilidade = 1e6, maxRentabilidade = -1e6;
                            ArrayList<Double> rentabilidades = new ArrayList<>();
                            ArrayList<Double> quotas = new ArrayList<>();
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
                                    if (numValores > 1) { //pois n-1 == 0
                                        desvioPadrao = Math.sqrt(sumDesvioPadrao / (numValores - 1));
                                    } else {
                                        desvioPadrao = 0.0;
                                    }
                                    double maxQuota = -1e6, drawDown, maxDrawdown = 0.0; // TODO: MaxQuota é muito negativa pois pode haver quotas negativas (check this out)
                                    for (Double quota : quotas) {
                                        if (quota > maxQuota) {
                                            maxQuota = quota;
                                        } else {
                                            if (maxQuota == 0.0) {
                                                drawDown = 0.0;
                                            } else {
                                                drawDown = quota / maxQuota - 1.0;
                                            }
                                            if (drawDown < maxDrawdown) {
                                                maxDrawdown = drawDown;
                                            }
                                        }
                                    }
                                    // salva no BD
                                    sql = "INSERT INTO indicadores_fundos (cnpj_fundo_id, periodo_meses, data_final, rentabilidade, desvio_padrao, num_valores, rentab_min, rentab_max, max_drawdown, tipo_benchmark_id, meses_acima_bench, sharpe, beta) VALUES (" + idFundo + "," + numMeses + ",'" + dataFinal + "'," + rentabilidadeMedia + "," + desvioPadrao + "," + numValores + "," + minRentabilidade + "," + maxRentabilidade + "," + maxDrawdown + ",null,null,null,null)";
                                    try {
                                        st2.execute(sql);
                                        countRecords++;
                                        if (numMeses % 12 == 0) {
                                            connection.commit();
                                            tempoProcessamentoParte = (System.currentTimeMillis() - fimProcessamento);
                                            tempoProcessamentoTotal = (System.currentTimeMillis() - inicioProcessamento);
                                            System.out.println("info: " + numMeses + " meses processados da última parte (" + (int) (tempoProcessamentoParte) + " ms)");//, sendo " + countRecordsComErro + " com erro e " + countRecordsRepetidos + " preexistentes");
                                            System.out.println("info: " + numMeses + " meses processados até o momento (totalizando " + (int) (tempoProcessamentoTotal) + " ms)");//, sendo " + countRecordsComErro + " com erro e " + countRecordsRepetidos + " preexistentes");
                                            fimProcessamento = System.currentTimeMillis();
                                        }
                                    } catch (SQLException ex) {
                                        // Logger.getLogger(CVMDatabaseThread.class.getName()).log(Level.SEVERE, null, ex);
                                        countRecordsRepetidos++;
                                    }
                                }
                            } while (rs.next() && numMeses <= 48);
                            connection.commit();
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(InvestFundsManager.class.getName()).log(Level.SEVERE, null, ex);
            countRecordsComErro++;
        }
        return new Object[]{countRecords, countRecordsComErro, countRecordsRepetidos};
    }
}