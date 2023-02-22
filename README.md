## Elastic Search by python の記事が少なかったので実際に実装してみました。

### 今回はDocker環境で構築したので docker-compose up -d で全く同じ環境を再現することができます。

(わーい dockerって素晴らしいぃ)

ElasticSearchについて、ここで解説するつもりはないので各自調べてください。(おそらくqiita書くときに少し触れると思う)

### ～実際に検索を行う手順～

1. docker-compose up -d
2. curl -X POST -H "Content-Type: application/json" localhost:8002/indices/product-index
3. curl -X GET "http://localhost:8002/create_test_data"
4. curl -X POST localhost:8002/indices/product-index/document -H "Content-Type: application/json" -d '{"keywords": {"address": ["沖縄"]}}'

※keywordsのaddress部分はcsvのheader値なら検索できます

※新しく検索データを作成したい場合は以下のコマンドを叩いてください
curl -X DELETE localhost:8002/indices/product-index

curl -X POST -H "Content-Type: application/json" localhost:8002/indices/product-index

curl -X POST "http://localhost:8002/create_dummy_data/10"
10の部分には任意の数字を入力できます。この数字文データが作成されます。




