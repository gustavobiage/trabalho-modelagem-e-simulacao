import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TestSharpeFundo {
    private static final BigDecimal EPSILON = new BigDecimal("1e-3");
    /*
     *   |---------------------------------------|
     *   | rent. diár. fund. | rent. diár. merc. |
     *   |-------------------|-------------------|
     *   |        4,328      |        8,928      |
     *   |        9,521      |        2,349      |
     *   |        2,457      |        6,121      |
     *   |        5,826      |        4,460      |
     *   |-------------------|-------------------|
     *
     *
     *   rent. diár. méd. fund. = 5,5330
     *   rent. diár. méd. merc. = 5,4645
     *   volat. fund.           = 2,5934
     *   volat. merc.           = 2,55578
     *   índice sharpe          = 0,02641743368
     *   índice sharpe gen.     = 1,819728689
     *   índice beta            = -1,849906207
     */
    @Test
    public void testIndiceSharpe1() throws SQLException, ClassNotFoundException {
        List<BigDecimal> rentabilidadeDiariaFundos = Arrays.asList(
                new BigDecimal("4.328"),
                new BigDecimal("9.521"),
                new BigDecimal("2.457"),
                new BigDecimal("5.826"));
        List<BigDecimal> rentabilidadeDiariaMercado = Arrays.asList(
                new BigDecimal("8.928"),
                new BigDecimal("2.349"),
                new BigDecimal("6.121"),
                new BigDecimal("4.460"));
        BigDecimal volatilidadeMercado = new BigDecimal("2.55578");
        ConstantInvestFundsManager investFundsManager = new ConstantInvestFundsManager(rentabilidadeDiariaFundos, rentabilidadeDiariaMercado, volatilidadeMercado);
        // rent. diár. med. fund.
        BigDecimal rentabilidadeMediaFundo = investFundsManager.calcular_rentabilidade_media_fundo();
        BigDecimal rentMediaFundDiferenca = new BigDecimal("5.533").subtract(rentabilidadeMediaFundo).abs();
        Assert.assertTrue(rentMediaFundDiferenca.compareTo(EPSILON) <= 0);
        // volat. fund.
        BigDecimal volatilidadeFundo = investFundsManager.calcular_volatilidade_fundo();
        BigDecimal volatFundDiferenca = new BigDecimal("2.5934").subtract(volatilidadeFundo);
        Assert.assertTrue(volatFundDiferenca.compareTo(EPSILON) <= 0);
        // rent. diár. med. merc.
        BigDecimal rentabilidadeMediaMercado = investFundsManager.calcular_rentabilidade_media_mercado();
        BigDecimal rentMediaMercDiferenca = new BigDecimal("5.4645").subtract(rentabilidadeMediaMercado).abs();
        Assert.assertTrue(rentMediaMercDiferenca.compareTo(EPSILON) <= 0);
        // Sharpe
        BigDecimal sharpe = investFundsManager.calcular_sharpe_fundo();
        BigDecimal sharpeDiferenca = new BigDecimal("0.02641743368").subtract(sharpe).abs();
        Assert.assertTrue(sharpeDiferenca.compareTo(EPSILON) <= 0);
    }

    /*
     *   |---------------------------------------|
     *   | rent. diár. fund. | rent. diár. merc. |
     *   |-------------------|-------------------|
     *   |        6,511      |        5,128      |
     *   |        7,308      |        3,523      |
     *   |        4,574      |        4,498      |
     *   |        3,921      |        6,024      |
     *   |        2,995      |        3,216      |
     *   |        3,513      |        6,483      |
     *   |        2,297      |        5,826      |
     *   |        6,780      |        2,372      |
     *   |-------------------|-------------------|
     *
     *
     *   rent. diár. méd. fund. = 4,737375
     *   rent. diár. méd. merc. = 4,63375
     *   volat. fund.           = 1,77164224221
     *   volat. merc.           = 3,82367
     *   índice sharpe          = 0,05849092866
     *   índice sharpe gen.     = -0,05049882956
     *   índice beta            = -0,3404733623
     */
    @Test
    public void testIndiceSharpe2() throws SQLException, ClassNotFoundException {
        List<BigDecimal> rentabilidadeDiariaFundos = Arrays.asList(
                new BigDecimal("6.511"),
                new BigDecimal("7.308"),
                new BigDecimal("4.574"),
                new BigDecimal("3.921"),
                new BigDecimal("2.995"),
                new BigDecimal("3.513"),
                new BigDecimal("2.297"),
                new BigDecimal("6.780"));
        List<BigDecimal> rentabilidadeDiariaMercado = Arrays.asList(
                new BigDecimal("5.128"),
                new BigDecimal("3.523"),
                new BigDecimal("4.498"),
                new BigDecimal("6.024"),
                new BigDecimal("3.216"),
                new BigDecimal("6.483"),
                new BigDecimal("5.826"),
                new BigDecimal("2.372"));
        BigDecimal volatilidadeMercado = new BigDecimal("3.82367");
        ConstantInvestFundsManager investFundsManager = new ConstantInvestFundsManager(rentabilidadeDiariaFundos, rentabilidadeDiariaMercado, volatilidadeMercado);
        // rent. diár. med. fund.
        BigDecimal rentabilidadeMediaFundo = investFundsManager.calcular_rentabilidade_media_fundo();
        BigDecimal rentMediaFundDiferenca = new BigDecimal("4.737375").subtract(rentabilidadeMediaFundo).abs();
        Assert.assertTrue(rentMediaFundDiferenca.compareTo(EPSILON) <= 0);
        // volat. fund.
        BigDecimal volatilidadeFundo = investFundsManager.calcular_volatilidade_fundo();
        BigDecimal volatFundDiferenca = new BigDecimal("1.77164224221").subtract(volatilidadeFundo);
        Assert.assertTrue(volatFundDiferenca.compareTo(EPSILON) <= 0);
        // rent. diár. med. merc.
        BigDecimal rentabilidadeMediaMercado = investFundsManager.calcular_rentabilidade_media_mercado();
        BigDecimal rentMediaMercDiferenca = new BigDecimal("4.63375").subtract(rentabilidadeMediaMercado).abs();
        Assert.assertTrue(rentMediaMercDiferenca.compareTo(EPSILON) <= 0);
        // Sharpe
        BigDecimal sharpe = investFundsManager.calcular_sharpe_fundo();
        BigDecimal sharpeDiferenca = new BigDecimal("0.05849092866").subtract(sharpe).abs();
        Assert.assertTrue(sharpeDiferenca.compareTo(EPSILON) <= 0);
    }

    /*
     *   |---------------------------------------|
     *   | rent. diár. fund. | rent. diár. merc. |
     *   |-------------------|-------------------|
     *   |        0,543      |        7,880      |
     *   |        0,761      |        5,125      |
     *   |        9,858      |        9,755      |
     *   |        1,989      |        8,469      |
     *   |        5,600      |        6,811      |
     *   |        7,797      |        5,334      |
     *   |        8,404      |        4,668      |
     *   |        8,515      |        9,924      |
     *   |        9,042      |        5,805      |
     *   |        3,595      |        4,660      |
     *   |        8,373      |        8,506      |
     *   |        1,602      |        9,314      |
     *   |        8,313      |        9,173      |
     *   |        9,848      |        8,659      |
     *   |        2,931      |        3,581      |
     *   |        2,602      |        5,106      |
     *   |-------------------|-------------------|
     *
     *
     *   rent. diár. méd. fund. = 5,6108125
     *   rent. diár. méd. merc. = 7,048125
     *   volat. fund.           = 3,3764070626
     *   volat. merc.           = 4,19172
     *   índice sharpe          = -0,4256928958
     *   índice sharpe gen.     = 1,762896716
     *   índice beta            = 0,4862976948
     */
    @Test
    public void testIndiceSharpe3() throws SQLException, ClassNotFoundException {
        List<BigDecimal> rentabilidadeDiariaFundos = Arrays.asList(
                new BigDecimal("0.543"),
                new BigDecimal("0.761"),
                new BigDecimal("9.858"),
                new BigDecimal("1.989"),
                new BigDecimal("5.600"),
                new BigDecimal("7.797"),
                new BigDecimal("8.404"),
                new BigDecimal("8.515"),
                new BigDecimal("9.042"),
                new BigDecimal("3.595"),
                new BigDecimal("8.373"),
                new BigDecimal("1.602"),
                new BigDecimal("8.313"),
                new BigDecimal("9.848"),
                new BigDecimal("2.931"),
                new BigDecimal("2.602"));
        List<BigDecimal> rentabilidadeDiariaMercado = Arrays.asList(
                new BigDecimal("7.880"),
                new BigDecimal("5.125"),
                new BigDecimal("9.755"),
                new BigDecimal("8.469"),
                new BigDecimal("6.811"),
                new BigDecimal("5.334"),
                new BigDecimal("4.668"),
                new BigDecimal("9.924"),
                new BigDecimal("5.805"),
                new BigDecimal("4.660"),
                new BigDecimal("8.506"),
                new BigDecimal("9.314"),
                new BigDecimal("9.173"),
                new BigDecimal("8.659"),
                new BigDecimal("3.581"),
                new BigDecimal("5.106"));
        BigDecimal volatilidadeMercado = new BigDecimal("4.19172");
        ConstantInvestFundsManager investFundsManager = new ConstantInvestFundsManager(rentabilidadeDiariaFundos, rentabilidadeDiariaMercado, volatilidadeMercado);
        // rent. diár. med. fund.
        BigDecimal rentabilidadeMediaFundo = investFundsManager.calcular_rentabilidade_media_fundo();
        BigDecimal rentMediaFundDiferenca = new BigDecimal("5.6108125").subtract(rentabilidadeMediaFundo).abs();
        Assert.assertTrue(rentMediaFundDiferenca.compareTo(EPSILON) <= 0);
        // volat. fund.
        BigDecimal volatilidadeFundo = investFundsManager.calcular_volatilidade_fundo();
        BigDecimal volatFundDiferenca = new BigDecimal("3.3764070626").subtract(volatilidadeFundo);
        Assert.assertTrue(volatFundDiferenca.compareTo(EPSILON) <= 0);
        // rent. diár. med. merc.
        BigDecimal rentabilidadeMediaMercado = investFundsManager.calcular_rentabilidade_media_mercado();
        BigDecimal rentMediaMercDiferenca = new BigDecimal("7.048125").subtract(rentabilidadeMediaMercado).abs();
        Assert.assertTrue(rentMediaMercDiferenca.compareTo(EPSILON) <= 0);
        // Sharpe
        BigDecimal sharpe = investFundsManager.calcular_sharpe_fundo();
        BigDecimal sharpeDiferenca = new BigDecimal("-0.4256928958").subtract(sharpe).abs();
        Assert.assertTrue(sharpeDiferenca.compareTo(EPSILON) <= 0);
    }
}
