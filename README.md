# SOBRE #

O modelo de dados deve possuir ao menos dois campos, um id do tipo **uuid** e um insertion do tipo **timestamp**, qualquer outro campo deve ser especificado em **fields** no arquivo de configuração.

Atualmente o sistema é bem simples, conecta nas portas padrão do elastic e do cassandra.

# INSTALAÇÃO #

Crie um ambiente virtual com virtualenv e instale as dependêncis.

```sh
$ pip install -r requirements.txt
```

# CONFIGURAÇÃO #

O arquivo de configuração é um json com as seguintes opções:

```json
{
    "interval": 30,
    "fields": [
        "title",
        "body"
    ],
    "cassandra": {
        "keyspace": "blog",
        "table": "posts"
    },
    "elastic": {
        "index": "blog",
        "collection": "posts"
    }
}
```

# POSSIVEIS PROBLEMAS #

Estou usando ALLOW FILTERING no dataset inteiro no banco cassandra, isso pode ser algo problemático em um banco de dados com muitos dados.

Não estou persistindo a data da última alteração em nenhum local, se o script falhar, isso pode ser um problema também.

Fiz testes apenas da lógica principal.

Atualmente o sistema considera que todos os campos são do tipo string, que está longe do ideal. Em um sistema real, provavelmente eu implementaria um esquema de classes, parecido com o django para informar o tipo do dado.

Minha experiência com ambos os bancos de dados é limitado a esse experimento, provavelmente há formas mais adequadas para afzer esse procedimento.

O script foi testado apenas com **python 3.4**

# USO EM PRODUÇÃO #

Não faça uso deste script em produção, atualmente é apenas um experimento.

# SETUP ELASTIC #

Apenas inicie e já está pronto.

# SETUP CASSANDRA #

[Reference](http://planetcassandra.org/create-a-keyspace-and-table/)

```sql
CREATE KEYSPACE blog
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
```

```sql
USE blog;
```

[Reference](http://docs.datastax.com/en/cql/3.0/cql/cql_reference/cql_data_types_c.html)

```sql
CREATE TABLE posts (
  id uuid,
  insertion timestamp,
  title text,
  body text,
  PRIMARY KEY (id, insertion)
) WITH CLUSTERING ORDER BY (insertion DESC);
```
