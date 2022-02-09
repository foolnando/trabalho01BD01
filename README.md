# Trabalho 01 de BD1
### Equipe: Nabson, Marcos e Fernando

A base de dados pode ser baixada em: https://snap.stanford.edu/data/amazon-meta.html

Coloque o arquivo `amazon-meta.txt` nesta pasta.

## Comandos
Para criar buildar e criar um container usando o docker-compose:
```bash
make down build up
```
*Caso queira criar o container e liberar o terminal em seguida, use `make down build up-silent`*

Para abrir um terminal dentro do container em execução:
```bash
make shell
```

## Comandos dentro do container
Dentro do terminal do container, rode o comando abaixo para entrar na pasta com os dados do trabalho:
```bash
cd tp1_data
```

Rode o comando abaixo para criar o esquema do banco e populá-lo com os dados do arquivo `amazon-meta.txt`:
```bash
python3 tp1_3.2.py
```

Após popular o banco, rode o comando abaixo para gerar o _dashboard_:
```bash
python3 tp1_3.3.py
```