import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        try {
            InvestFundsManager investFundsManager = new InvestFundsManager(true);
            Object[] estatistica = investFundsManager.calcula_indicadores_DOC_INF_DIARIO();
            System.out.println("countRecords=" + estatistica[0] + ", countRecordsComErro=" + estatistica[1] + ", countRecordsRepetidos=" + estatistica[2]);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
