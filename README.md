go run main.go \
  -api http://localhost:8000 \
  -rules "regras_pricing.csv" \
  -coletas "entrada.csv" \
  -out "saida.csv" \
  -bootstrap=true \
  -prio-base 00001
