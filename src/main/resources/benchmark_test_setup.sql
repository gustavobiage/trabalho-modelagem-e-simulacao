-- mysql> SHOW COLUMNS FROM cadastro_fundos;
-- +-----------------------------+------------+------+-----+---------+-------+
-- | Field                       | Type       | Null | Key | Default | Extra |
-- +-----------------------------+------------+------+-----+---------+-------+
-- | cnpj_fundo_id               | int(11)    | NO   | PRI | NULL    |       |
-- | DT_CONST                    | date       | NO   |     | NULL    |       |
-- | tipo_classe_fundo_id        | int(11)    | NO   | PRI | NULL    |       |
-- | DT_INI_CLASSE               | date       | NO   | PRI | NULL    |       |
-- | tipo_rentabilidade_fundo_id | int(11)    | NO   | PRI | NULL    |       |
-- | CONDOM_ABERTO               | tinyint(1) | NO   | PRI | NULL    |       |
-- | FUNDO_COTAS                 | tinyint(1) | NO   | PRI | NULL    |       |
-- | FUNDO_EXCLUSIVO             | tinyint(1) | NO   | PRI | NULL    |       |
-- | INVEST_QUALIF               | tinyint(1) | NO   | PRI | NULL    |       |
-- | gestor_fundo_id             | int(11)    | NO   | PRI | NULL    |       |
-- | administrador_fundo_id      | int(11)    | NO   | PRI | NULL    |       |
-- | DT_REG_CVM                  | date       | NO   |     | NULL    |       |
-- +-----------------------------+------------+------+-----+---------+-------+

DROP TABLE IF EXISTS cadastro_fundos;

CREATE TABLE cadastro_fundos (
    cnpj_fundo_id INT(11),
    DT_CONST DATE,
    tipo_classe_fundo_id INT(11),
    DT_INI_CLASSE DATE,
    tipo_rentabilidade_fundo_id INT(11),
    CONDOM_ABERTO TINYINT(1),
    FUNDO_COTAS TINYINT(1),
    FUNDO_EXCLUSIVO TINYINT(1),
    INVEST_QUALIF TINYINT(1),
    gestor_fundo_id INT(11),
    administrador_fundo_id INT(11),
    DT_REG_CVM DATE,
    PRIMARY KEY(
                cnpj_fundo_id,
                tipo_classe_fundo_id,
                DT_INI_CLASSE,
                tipo_rentabilidade_fundo_id,
                CONDOM_ABERTO,
                FUNDO_COTAS,
                FUNDO_EXCLUSIVO,
                INVEST_QUALIF,
                gestor_fundo_id,
                administrador_fundo_id,
                DT_REG_CVM
               )
);

-- +-------+------------+---+------------+---+---+---+---+---+------+-----+------------+
-- | 38494 | 2020-10-26 | 2 | 2020-10-26 | 2 | 1 | 0 | 1 | 1 |    1 |   1 | 2020-10-27 |
-- | 38495 | 2020-10-26 | 1 | 2020-10-26 | 2 | 1 | 0 | 0 | 0 |  439 |  14 | 2020-10-27 |
-- | 38496 | 2020-10-27 | 3 | 2020-10-27 | 4 | 1 | 1 | 0 | 0 |   11 |   4 | 2020-10-27 |
-- +-------+------------+---+------------+---+---+---+---+---+------+-----+------------+

-- (cnpj_fundo_id, DT_CONST, tipo_classe_fundo_id, DT_INI_CLASSE, tipo_rentabilidade_fundo_id, CONDOM_ABERTO, FUNDO_COTAS, INVEST_QUALIF, gestor_fundo_id, administrador_fundo_id, DT_REG_CVM)
INSERT INTO cadastro_fundos VALUES
    (38494, '2020-10-26', 2, '2020-10-26', 2, 1, 0, 1, 1, 1, 1, '2020-10-27');

INSERT INTO cadastro_fundos VALUES
    (38495, '2020-10-26', 1, '2020-10-26', 2, 1, 0, 0, 0, 439, 14, '2020-10-27');

INSERT INTO cadastro_fundos VALUES
    (38496, '2020-10-27', 3, '2020-10-27', 4, 1, 1, 0, 0, 11, 4, '2020-10-27');

INSERT INTO cadastro_fundos VALUES
    (38394, '2020-9-26', 2, '2020-9-26', 2, 1, 0, 1, 1, 1, 1, '2020-9-27');

INSERT INTO cadastro_fundos VALUES
    (38395, '2020-9-26', 1, '2020-9-26', 2, 1, 0, 0, 0, 419, 14, '2020-9-27');

INSERT INTO cadastro_fundos VALUES
    (38396, '2020-9-27', 3, '2020-9-27', 4, 1, 1, 0, 0, 11, 4, '2020-9-27');

INSERT INTO cadastro_fundos VALUES
    (38594, '2020-8-26', 2, '2020-8-26', 2, 1, 0, 1, 0, 1, 0, '2020-8-27');

INSERT INTO cadastro_fundos VALUES
    (38595, '2020-8-26', 1, '2020-8-26', 2, 1, 1, 0, 0, 429, 14, '2020-8-27');

INSERT INTO cadastro_fundos VALUES
    (38596, '2020-8-27', 3, '2020-8-27', 4, 1, 0, 0, 0, 11, 4, '2020-8-27');


-- Create this table on a new database to test benchmark.sql script
--      Foreign key on inserted values should be random in range [0, 4]