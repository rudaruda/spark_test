# spark_test
Teste Spark

1. Qual o objetivo do comando **cache** em Spark?
```
RESPOSTA
O comando "cache()" realizará o armazenameno do RDD na memória enquanto o comando "persist(level: StorageLevel)" fará o armazenamento no disco. E para limpar a mémoria do cache fazemos uso do comando "unpersist()".
Com essas opções podemos fazer o balanceamento de memória para ganho de performance da aplicação.
```
2. O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?
```
RESPOSTA
Porque a arquitetura do Spark permite o armazenamento do estado de mémoria como objeto (RDDs) e isso multiplica seu poder de processamento.
```
3. Qual é a função do **SparkContext**?
```
RESPOSTA
É um objeto de controle do Spark como serviço e parametrização dos atributos de ambiente de produção/desenvolvimeno.
```
4. Explique com suas palavras o que é **Resilient Distributed Datasets** (RDD).
```
RESPOSTA
Resilient Distributed Datasets = Conjunto de dados distribuídos resilientes
É um objeto de big data formado por uma coleção de registros somente para leitura com armazenado paralelo (para realizar processamentos paralelo com o HDFS).
```
5. **groupByKey** é menos eficiente que **reduceByKey** em grandes dataset. Por quê?
```
RESPOSTA
groupByKey faz agregação dos valores.
reduceByKey faz agregação dos valores indexados.
```
6. Explique o que o código Scala abaixo faz.
>val textFile = sc.textFile("hdfs://...")<br>val counts = textFile.flatMap(line => line.split(" "))<br>.map(word => (word, 1))<br>.reduceByKey(_ + _)<br>counts.saveAsTextFile("hdfs://...")
```
RESPOSTA
  > val textFile = sc.textFile("hdfs://...")
    Cria o RDD "textFile" a apartir de leitura de arquivo externo (ou seja, lê o arquivo do HDFS com spark)
  > val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    Seta variavel "counts", esta fazendo map reduce, para obter a contagem de cada palavra no arquivo
  > counts.saveAsTextFile("hdfs://...")
    Exporta objeto para arquivo HDFS.
```
