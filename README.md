# Fraud-Detection
$line = Get-Content payments.jsonl -TotalCount 1
Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/authorize `
  -ContentType "application/json" -Body $line

Get-Content payments.jsonl -TotalCount 100 | ForEach-Object {
  $r = Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/authorize `
         -ContentType "application/json" -Body $_
  $r.decision
} | Group-Object | Select-Object Name, Count




# Take only the first line of the file
$line = Get-Content .\transactions.jsonl -TotalCount 1

# Send it through HAProxy to the decision service
Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/decide `
  -ContentType "application/json" -Body $line



docker compose run --rm `
  -v "${PWD}/tools:/tools:ro" -v "${PWD}/cards.jsonl:/cards.jsonl:ro" `
  -e CARDS_FILE=/cards.jsonl decision-service python /tools/seed.py

  python txn_generator.py --tps 100 --duration 60 --seed 42 --speedup 1 --url http://localhost:8080/v1/decide



  docker compose run --rm -v "${PWD}/tools:/tools:ro" decision-service python /tools/counter.py