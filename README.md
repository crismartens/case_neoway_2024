# Serviço de Manipulação de Dados e Persistência

Este serviço foi desenvolvido para manipular e persistir dados em um banco de dados relacional. 
O objetivo principal é receber um arquivo CSV ou TXT como entrada e persistir os dados contidos neste arquivo em um banco de dados PostgreSQL. 
Este serviço foi desenvolvido em Python e utiliza Docker Compose para toda a infraestrutura.

## Pré-requisitos:

Antes de executar este serviço, certifique-se de que você tenha o Docker instalado no seu sistema. 
Você pode baixar e instalar o Docker a partir do [site oficial do Docker](https://www.docker.com/get-started).

## Instruções de Uso:

Para iniciar o ambiente, execute um dos seguintes comandos no terminal, dependendo das suas necessidades:

Opção 1: 

- **Criar e executar todos os contêineres:**
  ```bash
  docker-compose up
  
- **Executar o serviço novamente:**
  ```bash
  docker-compose start app
  
Opção 2: 

- **Criar os conteineres:**
  ```bash
  docker-compose create

- **Iniciar o PostgreSQL::**
  ```bash
  docker-compose start postgres

- **Executar/Reexecutar o Serviço::**
  ```bash
  docker-compose start app
