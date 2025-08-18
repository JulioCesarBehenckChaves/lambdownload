# lambdownload
Lambda Function to download files directly into a S3 bucket.

Roteiro no AWS Academy:

Script CloudFormation para criar função lambda, bucket, job glue e ambiente athena, além de acessos.

Exemplo de download de:
https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao/resource/301983f2-aa50-4977-8fec-cfab0806cb0b

https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/completo/part-00000-70dd7710-b64c-4a6e-a780-bf4ca7d0a1f7-c000.csv

https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/completo/part-00000-04a13428-bf17-4980-8077-73151cc098e3-c000.csv


Recursos principais:

S3 Bucket: Para armazenar os arquivos baixados

IAM Role: Com permissões mínimas para Lambda e S3

Lambda Function: Que baixa o repositório GitHub como ZIP e extrai para S3

Como usar:

Altere os parâmetros no arquivo ou no comando de deploy:

GitHubRepo: formato "owner/repository"

S3BucketName: nome único para o bucket

Teste a função:

aws lambda invoke --function-name github-download-function output.json

1º teste
```json
{
    "url": "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/completo/part-00000-04a13428-bf17-4980-8077-73151cc098e3-c000.csv",
    "bucket": "administrativoticlab",
    "prefix": "landing-zone/",
    "filename": "covid-p0.csv"
}
```