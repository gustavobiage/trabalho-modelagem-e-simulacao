DROP TABLE IF EXISTS tipo_benchmarks;

CREATE TABLE tipo_benchmarks (
    benchmark_id INT AUTO_INCREMENT PRIMARY KEY,
    benchmark_name VARCHAR(255) NOT NULL
);

INSERT INTO tipo_benchmarks (benchmark_id, benchmark_name) VALUES (1, 'SELIC');
INSERT INTO tipo_benchmarks (benchmark_id, benchmark_name) VALUES (2, 'CDI');
INSERT INTO tipo_benchmarks (benchmark_id, benchmark_name) VALUES (3, 'IPCA');
INSERT INTO tipo_benchmarks (benchmark_id, benchmark_name) VALUES (4, 'IBOVESPA');
INSERT INTO tipo_benchmarks (benchmark_id, benchmark_name) VALUES (5, 'USD/BRL');

ALTER TABLE cadastro_fundos
ADD COLUMN tipo_benchmark_id INT DEFAULT 1;

UPDATE cadastro_fundos SET tipo_benchmark_id = FLOOR(1 + RAND() * 5);

ALTER TABLE cadastro_fundos
    ADD FOREIGN KEY (tipo_benchmark_id) REFERENCES tipo_benchmarks(benchmark_id);
