# spark_test
Teste Spark

1. Qual o objetivo do comando **cache** em Spark?
```
RESPOSTA<br>
O comando "cache()" realizará o armazenameno do RDD na memória enquanto o comando <br>
"persist(level: StorageLevel)" fará o armazenamento no disco. E para limpar a mémoria do <br>
cache fazemos uso do comando "unpersist()".<br>
Com essas opções podemos fazer o balanceamento de memória para ganho de performance da aplicação.
```
2. O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?
```
RESPOSTA<br>
Porque a arquitetura do Spark permite o armazenamento do estado de mémoria como objeto (RDDs) e<br>
isso multiplica seu poder de processamento.
```
3. Qual é a função do **SparkContext**?
```
RESPOSTA<br>
É um objeto de controle do Spark como serviço e parametrização dos atributos de ambiente de<br>
produção/desenvolvimeno.
```
4. Explique com suas palavras o que é **Resilient Distributed Datasets** (RDD).
```
RESPOSTA<br>
Resilient Distributed Datasets = Conjunto de dados distribuídos resilientes<br>
É um objeto de big data formado por uma coleção de registros somente para leitura com<br>
armazenado paralelo (para realizar processamentos paralelo com o HDFS).
```
5. **groupByKey** é menos eficiente que **reduceByKey** em grandes dataset. Por quê?
```
RESPOSTA<br>
groupByKey faz agregação dos valores.<br>
reduceByKey faz agregação dos valores indexados.
```
6. Explique o que o código Scala abaixo faz.
>val textFile = sc.textFile("hdfs://...")<br>val counts = textFile.flatMap(line => line.split(" "))<br>.map(word => (word, 1))<br>.reduceByKey(_ + _)<br>counts.saveAsTextFile("hdfs://...")
```
RESPOSTA<br>
  > val textFile = sc.textFile("hdfs://...")<br>
    Cria o RDD "textFile" a apartir de leitura de arquivo externo (ou seja, lê o arquivo do HDFS com spark)<br>
  > val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)<br>
    Seta objeto "counts", que esta fazendo map reduce, para obter a contagem de cada string<br>separada por " " (espaço) no arquivo (ou seja, a contagem das palavras contidas no arquivo)
  > counts.saveAsTextFile("hdfs://...")<br>
    Exporta objeto para arquivo HDFS.<br>
```
