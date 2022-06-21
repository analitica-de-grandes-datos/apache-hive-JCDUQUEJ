/* 

Pregunta
===========================================================================

Escriba una consulta que retorne unicamente la columna t0.c5 con sus 
elementos en mayuscula.

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

*/

DROP TABLE IF EXISTS tbl0;
CREATE TABLE tbl0 (
    c1 INT,
    c2 STRING,
    c3 INT,
    c4 DATE,
    c5 ARRAY<CHAR(1)>, 
    c6 MAP<STRING, INT>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
MAP KEYS TERMINATED BY '#'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data0.csv' INTO TABLE tbl0;

DROP TABLE IF EXISTS tbl1;
CREATE TABLE tbl1 (
    c1 INT,
    c2 INT,
    c3 STRING,
    c4 MAP<STRING, INT>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
MAP KEYS TERMINATED BY '#'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data1.csv' INTO TABLE tbl1;

/*
    >>> Escriba su respuesta a partir de este punto <<<
*/

DROP TABLE IF EXISTS letras;
CREATE TABLE letras (linea INT, letra STRING);

INSERT OVERWRITE TABLE letras
SELECT
        tbl0.c1,
        UPPER(m.c7)
FROM
        tbl0
LATERAL VIEW
        explode(c5) m AS c7;

DROP TABLE IF EXISTS resultados;
CREATE TABLE resultados (letra1 STRING);

INSERT OVERWRITE TABLE resultados
SELECT
        concat_ws(':', collect_set(letra))
FROM
        letras
GROUP BY
        linea;


INSERT OVERWRITE DIRECTORY './output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM resultados;