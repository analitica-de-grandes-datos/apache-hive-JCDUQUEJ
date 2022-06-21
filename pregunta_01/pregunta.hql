/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Compute la cantidad de registros por cada letra de la columna 1.
Escriba el resultado ordenado por letra. 

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/

DROP TABLE IF EXISTS datos;
DROP TABLE IF EXISTS datos_raw;
DROP TABLE IF EXISTS resultados;

CREATE TABLE datos_raw (linea STRING);
CREATE TABLE datos (letra STRING, fecha STRING, numero INT);
CREATE TABLE resultados (clave STRING, suma INT);

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
SELECT
        letra,
        count(letra)
FROM
        datos
GROUP BY
        letra
ORDER BY
        letra;


INSERT OVERWRITE DIRECTORY '/output/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM resultados;