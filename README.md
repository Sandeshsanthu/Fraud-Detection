python -m unittest tests.test_logic -v

python -m tests.offline_sim

python txn_generator.py --tps 100 --duration 60 --seed 42 --out data/payments.jsonl --labels data/labels.jsonl --cards-out data/cards.jsonl

python txn_generator.py --tps 100 --duration 60 --seed 42 --speedup 1 --timeout-ms 300 --url http://localhost:8080/v1/decide --out data/payments.jsonl --labels data/labels.jsonl --cards-out data/cards.jsonl

docker compose run --rm --no-deps setup python -m scripts.check

docker compose stop 

python scripts/smoke.py --url http://localhost:8080/v1/decide
docker compose start 
$env:FEATURE_MODE="poller"; docker compose --profile poller up -d --build
