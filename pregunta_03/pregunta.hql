/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Escriba una consulta que devuelva los cinco valores diferentes más pequeños 
de la tercera columna.

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/

DROP TABLE IF EXISTS datos;
DROP TABLE IF EXISTS datos_raw;
DROP TABLE IF EXISTS resultados;

CREATE TABLE datos_raw (linea STRING);
CREATE TABLE datos (letra STRING, fecha STRING, numero INT);
CREATE TABLE resultados (numero INT);

LOAD DATA LOCAL INPATH "data.tsv" OVERWRITE INTO TABLE datos_raw;

INSERT OVERWRITE TABLE datos
SELECT
        SPLIT(linea,'\t')[0] letra,
        SPLIT(linea,'\t')[1] fecha,
        SPLIT(linea,'\t')[2] numero
FROM
        datos_raw
LIMIT 40;

INSERT OVERWRITE TABLE resultados
SELECT DISTINCT
        numero
FROM
        datos
ORDER BY
        numero
LIMIT 5;

INSERT OVERWRITE DIRECTORY './output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM resultados;