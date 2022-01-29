public class ExecutionInformation {
    /**
     * Inicio do processamento realizado nos dados do banco
     */
    private Long inicioProcessamento;
    /**
     * Tempo de processamento da última parte executada
     */
    private Long tempoProcessamentoParte;
    /**
     * Tempo de processamento total da execução
     */
    private Long tempoProcessamentoTotal;
    /**
     * Fim do processamento realizado nos dados do banco
     */
    private Long fimProcessamento;
    /**
     * Quantidade de linhas na tabelas atualizadas no banco
     */
    private Long countRecords;
    /**
     * Quantidade de linhas na tabelas não atualizadas por um erro
     */
    private Long countRecordsComErro;
    /**
     * Quantidade de linhas já atualizadas no passado
     */
    private Long countRecordsRepetidos;

    /**
     * Construtor
     */
    public ExecutionInformation() {
        resetar();
    }
    /**
     * Reseta os dados
     */
    public void resetar() {
        this.inicioProcessamento = System.currentTimeMillis();
        this.tempoProcessamentoParte = this.inicioProcessamento;
        this.tempoProcessamentoTotal = this.inicioProcessamento;
        this.fimProcessamento = this.inicioProcessamento;
        this.countRecords = 0L;
        this.countRecordsComErro = 0L;
        this.countRecordsRepetidos = 0L;
    }
    /**
     * Obtém a quantidade de linhas atualizadas no baco
     * @return quantidade de linhas atualizada
     */
    public Long getCountRecords() {
        return this.countRecords;
    }
    /**
     * Obtém o início do processamento
     * @return início do processamento
     */
    public Long getInicioProcessamento() {
        return this.inicioProcessamento;
    }
    /**
     * Obtém o fim do processamento até agora
     * @return fim do processamento
     */
    public Long getFimProcessamento() {
        return fimProcessamento;
    }
    /**
     * Obtém o tempo de processamento da última parte
     * @return tempo de processamento da útima parte
     */
    public Long getTempoProcessamentoParte() {
        return tempoProcessamentoParte;
    }
    /**
     * Obtém o tempo de processamento total até agora
     * @return tempo de processamento total
     */
    public Long getTempoProcessamentoTotal() {
        return tempoProcessamentoTotal;
    }

    /**
     * Retorna um array com os contadores sobre as linhas operadas no banco
     * @return retorna um array com o número de linhas operadas com sucesso, com erro e repetidas
     */
    public Object[] getRecordsArray() {
        return new Object[] {this.countRecords, this.countRecordsComErro, this.countRecordsRepetidos};
    }
    /**
     * Armazena o tempo de processamento da última parte
     */
    public void setTempoProcessamentoParte(Long tempoProcessamentoParte) {
        this.tempoProcessamentoParte = tempoProcessamentoParte;
    }
    /**
     * Armazena o tempo de processamento total
     */
    public void setTempoProcessamentoTotal(Long tempoProcessamentoTotal) {
        this.tempoProcessamentoTotal = tempoProcessamentoTotal;
    }
    /**
     * Armaza o fim do processamento
     */
    public void setFimProcessamento(Long fimProcessamento) {
        this.fimProcessamento = fimProcessamento;
    }
    /**
     * Incrementa o número de linhas atualizada
     */
    public void incrementCountRecords() {
        this.countRecords++;
    }
    /**
     * Incrementa o número de linhas repetidas
     */
    public void incrementCountRecordsRepetido() {
        this.countRecordsRepetidos++;
    }
    /**
     * Incrementa o númro de linhas com erro
     */
    public void incrementCountRecordsComErro() {
        this.countRecordsComErro++;
    }
}
