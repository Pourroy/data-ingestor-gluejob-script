# Fluxos de execução
  - Carga inicial
  - Modo incremental


## Parameters
| Parametro | Valor | Modo de execução |
| --------- | ----- | ---------------- |
| JobTriggerOrigin | OnDemand | Carga Inicial |
| JobTriggerOrigin | Scheduled | Modo Incremental |

| Parametro | Valor | Tabelas afetadas |
| --------- | ----- | ---------------- |
| JobMode | allTables | Todas as tabelas |
| JobMode | nome da tabela | Somente a tabela requisitada |

 Qualquer combinação não contemplada entre essas duas tabelas fará com que o job faça nada.


# Carga inicial
Nesse fluxo, serão lidos os arquivos csv no s3 load/sample/sample/{database}/{table}.csv
Sendo que database pode assumir os valores sample ou sample.

A partir deles, serão criados novos arquivos parquet nas pastas:
> - "raw/sample/{table}/company=sample/{table}_year={year}/{table}_month={month}/{table}.parquet"

# Modo incremental
Nesse fluxo, serão pesquisados no banco, a partir da data salva no parameter store - que corresponde a ultima execução com sucesso daquela tabela - os dados das tabelas informadas pelo parametro JobMode

A partir deles, serão criados novos arquivos parquet nas pastas:
> - "raw/sample/{table}/company=sample/{table}_year={year}/{table}_month={month}/{table}.parquet"



# Execução local

No arquivo notebook.ipynb, mantenha a primeira célula com o código atualizado do gluejob.

Para executar localmente, basta utilizar a segunda célula.