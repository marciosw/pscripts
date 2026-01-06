# VerPush - Scanner de Pacientes Firestore

Script CLI para varrer pacientes no Firestore e identificar aqueles que possuem a collection `e-tib-notificacoes`.

## Instalação

```bash
pip install -r requirements.txt
```

## Uso

### Forma básica:
```bash
python verpush.py key-prod.json output.csv
```

### Com opções:
```bash
python verpush.py --key-file key-prod.json --output output.csv --workers 100
```

### Parâmetros:

- `key_file`: Caminho para o arquivo JSON com credenciais do Firebase (obrigatório)
- `output_file`: Nome do arquivo CSV de saída (obrigatório)
- `--workers`: Número de workers para processamento paralelo (padrão: 50)
- `--batch-size`: Tamanho do lote para listar usuários do Auth (padrão: 1000)

## Como funciona

1. **Lista UIDs do Firebase Authentication**: O script usa o Firebase Auth para obter todos os UIDs de pacientes, evitando sobrecarregar o Firestore com leituras massivas.

2. **Processamento Paralelo**: Para cada UID, verifica em paralelo se existe a collection `e-tib-notificacoes` no caminho:
   ```
   /Tenant/vWxkIvCXIJUSIlhBSiIl/Patient/{uid}/e-tib-notificacoes
   ```

3. **Extração de CPF**: Se a collection existir, o script lê o documento do paciente e extrai o CPF (tentando vários campos possíveis).

4. **Geração de CSV**: Os resultados são salvos em um arquivo CSV com as colunas `uid` e `cpf`.

## Estrutura do CSV de saída

```csv
uid,cpf
abc123,12345678901
def456,98765432109
```

## Notas

- O script processa mais de 1.5 milhão de pacientes usando processamento paralelo
- Usa rate limiting implícito através do número de workers configurável
- Trata erros individualmente sem interromper o processamento completo
- Mostra progresso em tempo real durante a execução

