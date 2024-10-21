# Desenvolvimento de uma Metodologia para Estimativa de Perdas Energéticas em Usinas Fotovoltaicas por Falhas em _Trackers_ Utilizando Aprendizado de Máquina

Repositório destinado para o desenvolvimento do meu Trabalho de Conclusão de Curso.

## Requisitos
- Acesso ao repositório [pvIFSC](https://github.com/j-Lago/pvIFSC);
- [Python](https://www.python.org/) 3.12.3 ou superior;
- Acesso ao [Drive](https://drive.google.com/file/d/1kKfMldczB6TUfQgkRfr-tDqvCvMVX4If/view?usp=drive_link).

> **Observação**: Deverá ser adicionada a pasta `datalake` em `resources/`

## Instalação
1. Instale a biblioteca `pvIFSC` na raiz do projeto
    ```bash
    git clone https://github.com/j-Lago/pvIFSC.git
    ```

2. Crie o ambiente virtual `python`
    ```bash
    python3 -m venv .venv
    ```

3. Ative o ambiente

    ### Linux
    ```bash
    source .venv/bin/activate
    ```

    ### Windows
    ```bash
    .venv\Scripts\activate
    ```
    
4. Baixe as dependências
    ```bash
    pip install -r requirements.txt
    ```

## Povoando o `datawarehouse`

Execute o arquivo `main.py`

```bash
python3 services/db/main.py
```

## _Dashboards_

Para visualizar as _dashboards_, execute o arquivo `Home.py`

```bash
streamlit run services/frontend/Home.py
```