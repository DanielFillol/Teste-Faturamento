package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ================= Tipos (API) =================

type RuleAPI struct {
	ID              string            `json:"id"`
	EstablishmentID string            `json:"establishment_id"`
	Condicao        string            `json:"condicao"`
	Formula         string            `json:"formula"`
	Prioridade      int               `json:"prioridade"`
	Variaveis       map[string]string `json:"variaveis"`
}

type VariableAPI struct {
	ID       string `json:"id"`
	Nome     string `json:"nome"`
	TipoDado string `json:"tipo_dado"`
}

type CompleteSimReq struct {
	Contexto map[string]any `json:"contexto"`
}

type SimulateResponse struct {
	EstablishmentID  string        `json:"establishment_id"`
	EventoExternalID string        `json:"evento_external_id"`
	Valor            float64       `json:"valor"`
	Audit            []AuditResult `json:"audit"`
}

type AuditResult struct {
	RefRegra struct {
		Condicao string `json:"condicao"`
		Formula  string `json:"formula"`
		ID       string `json:"id"`
	} `json:"ref_regra"`
	AvaliacaoFormula *struct {
		Sucesso   bool     `json:"sucesso"`
		Resultado *float64 `json:"resultado"`
	} `json:"avaliacao_formula"`
	AvaliacaoCondicao struct {
		Sucesso   bool `json:"sucesso"`
		Resultado bool `json:"resultado"`
		Mensagem  any  `json:"mensagem"`
	} `json:"avaliacao_condicao"`
	Contexto map[string]any `json:"contexto"`
}

type APIError struct {
	Error             string        `json:"error"`
	Detail            string        `json:"detail"`
	EstablishmentID   string        `json:"establishment_id"`
	EventoExternalID  string        `json:"evento_external_id"`
	Audit             []AuditResult `json:"audit"`
	AvaliacaoSemValor bool          `json:"-"`
	HTTPStatus        int           `json:"-"`
	RawBody           string        `json:"-"`
}

// ================= Regras (CSV) =================

type RuleKey struct {
	Estab        string
	Material     string // material_kind_normalizado (ou material_kind); "" = coringa (qualquer material)
	ServiceType  string
	PricingModel string
}

type PricingSpec struct {
	Key           RuleKey
	UnitPrice     float64
	MinCollectFee float64
	MinQty        float64
	FreightFee    float64
	TS            time.Time
}

type MonthlySpec struct {
	Fee          float64
	IsAdditional bool
	TS           time.Time
	Has          bool
}

type Job struct {
	Idx int
	Row []string
}
type Result struct {
	Idx int
	Out []string
	Err error
}

// ================= Util =================

func upperTrim(s string) string { return strings.ToUpper(strings.TrimSpace(s)) }

func parseIntLoose(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	s = strings.ReplaceAll(s, " ", "")
	var b strings.Builder
	for _, r := range s {
		if (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		}
	}
	v, _ := strconv.Atoi(b.String())
	return v
}

func parseNumberLoose(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	s = strings.Trim(s, `"'`)
	s = strings.ReplaceAll(s, " ", "")

	hasDot := strings.Contains(s, ".")
	hasComma := strings.Contains(s, ",")

	if hasDot && hasComma {
		lastDot := strings.LastIndex(s, ".")
		lastComma := strings.LastIndex(s, ",")
		if lastComma > lastDot {
			// BR: "." milhar, "," decimal
			s = strings.ReplaceAll(s, ".", "")
			s = strings.ReplaceAll(s, ",", ".")
		} else {
			// US: "," milhar, "." decimal
			s = strings.ReplaceAll(s, ",", "")
		}
	} else if hasComma && !hasDot {
		// "," decimal
		s = strings.ReplaceAll(s, ".", "")
		s = strings.ReplaceAll(s, ",", ".")
	} else {
		// só "." decimal ou nenhum separador
		s = strings.ReplaceAll(s, ",", "")
	}

	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
}

func formatBR2(f float64) string {
	f = float64(int(f*100+0.5)) / 100
	s := fmt.Sprintf("%.2f", f)
	return strings.ReplaceAll(s, ".", ",")
}

func parseTS(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	layouts := []string{
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05.9999999",
		"2006-01-02 15:04:05",
	}
	for _, ly := range layouts {
		if t, err := time.Parse(ly, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ================= CSV read/write =================

func readCSV(path string) (comma rune, header []string, rows [][]string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return ',', nil, nil, err
	}
	defer f.Close()

	br := bufio.NewReader(f)
	peek, _ := br.Peek(4096)
	first := string(peek)

	switch {
	case strings.Contains(first, "\t") && !strings.Contains(first, ","):
		comma = '\t'
	case strings.Contains(first, ";") && !strings.Contains(first, ","):
		comma = ';'
	default:
		comma = ','
	}

	r := csv.NewReader(br)
	r.Comma = comma
	r.FieldsPerRecord = -1

	all, err := r.ReadAll()
	if err != nil {
		return comma, nil, nil, err
	}
	if len(all) == 0 {
		return comma, nil, nil, fmt.Errorf("CSV vazio")
	}
	header = all[0]
	rows = all[1:]
	return comma, header, rows, nil
}

func writeCSV(path string, comma rune, rows [][]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	w.Comma = comma
	for _, r := range rows {
		if err := w.Write(r); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func headerIndex(header []string) map[string]int {
	m := make(map[string]int, len(header))
	for i, h := range header {
		m[strings.ToLower(strings.TrimSpace(h))] = i
	}
	return m
}

func get(row []string, h map[string]int, col string) string {
	idx, ok := h[strings.ToLower(col)]
	if !ok || idx < 0 || idx >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[idx])
}

func getAny(row []string, h map[string]int, cols ...string) string {
	for _, c := range cols {
		v := get(row, h, c)
		if v != "" {
			return v
		}
	}
	return ""
}

// ================= HTTP =================

func newHTTPClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	tr := &http.Transport{
		DialContext:           dialer.DialContext,
		MaxIdleConns:          2000,
		MaxIdleConnsPerHost:   2000,
		MaxConnsPerHost:       2000,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &http.Client{Timeout: timeout, Transport: tr}
}

func doJSON(client *http.Client, method, url string, body any, out any, retries int, sem chan struct{}) (int, []byte, error) {
	var bodyBytes []byte
	var err error
	if body != nil {
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return 0, nil, fmt.Errorf("marshal request body: %w", err)
		}
	}

	for r := 0; r < retries+1; r++ {
		sem <- struct{}{}
		var reqBody io.Reader
		if body != nil {
			reqBody = bytes.NewReader(bodyBytes)
		}

		req, err := http.NewRequestWithContext(context.Background(), method, url, reqBody)
		if err != nil {
			<-sem
			return 0, nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		<-sem
		if err != nil {
			if r < retries {
				time.Sleep(time.Duration(r+1) * 2 * time.Second)
				continue
			}
			return 0, nil, fmt.Errorf("http request: %w", err)
		}
		defer resp.Body.Close()

		respBody, _ := io.ReadAll(resp.Body)

		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			if r < retries {
				time.Sleep(time.Duration(r+1) * 2 * time.Second)
				continue
			}
			return resp.StatusCode, respBody, fmt.Errorf("http status %d for %s", resp.StatusCode, url)
		}
		if resp.StatusCode >= 400 {
			return resp.StatusCode, respBody, errors.New(string(respBody))
		}

		if out != nil {
			if err := json.Unmarshal(respBody, out); err != nil {
				return resp.StatusCode, respBody, nil
			}
		}
		return resp.StatusCode, respBody, nil
	}
	return 0, nil, errors.New("max retries reached")
}

func listVariables(client *http.Client, apiBase string, retries int, sem chan struct{}) ([]VariableAPI, error) {
	url := strings.TrimRight(apiBase, "/") + "/variavel/"
	var out []VariableAPI
	_, _, err := doJSON(client, "GET", url, nil, &out, retries, sem)
	return out, err
}

func createVariable(client *http.Client, apiBase, nome, tipo string, retries int, sem chan struct{}) error {
	url := strings.TrimRight(apiBase, "/") + "/variavel/"
	body := map[string]any{"nome": nome, "tipo_dado": tipo}
	_, _, err := doJSON(client, "POST", url, body, nil, retries, sem)
	if err != nil && strings.Contains(err.Error(), "409") {
		return nil
	}
	return err
}

func listRulesByEstab(client *http.Client, apiBase, estab string, retries int, sem chan struct{}) ([]RuleAPI, error) {
	url := strings.TrimRight(apiBase, "/") + fmt.Sprintf("/regra/estabelecimento/%s", estab)
	var out []RuleAPI
	_, _, err := doJSON(client, "GET", url, nil, &out, retries, sem)
	return out, err
}

func createRule(client *http.Client, apiBase string, estab string, cond string, formula string, prio int, retries int, sem chan struct{}) error {
	url := strings.TrimRight(apiBase, "/") + "/regra/"
	body := map[string]any{
		"establishment_id": estab,
		"condicao":         cond,
		"formula":          formula,
		"prioridade":       prio,
	}
	_, _, err := doJSON(client, "POST", url, body, nil, retries, sem)
	return err
}

func simulate(client *http.Client, apiBase string, ctx map[string]any, retries int, sem chan struct{}) (*SimulateResponse, *APIError) {
	url := strings.TrimRight(apiBase, "/") + "/faturamento/simular"
	reqBody := CompleteSimReq{Contexto: ctx}

	var ok SimulateResponse
	status, raw, err := doJSON(client, "POST", url, reqBody, &ok, retries, sem)
	if err == nil {
		return &ok, nil
	}

	apiErr := &APIError{HTTPStatus: status, RawBody: string(raw)}
	_ = json.Unmarshal(raw, apiErr)
	if apiErr.Error == "" && apiErr.Detail == "" {
		apiErr.Detail = strings.TrimSpace(string(raw))
	}
	return nil, apiErr
}

// ================= Bootstrap: Variáveis + Dump =================

func ensureVariables(client *http.Client, apiBase string, retries int, sem chan struct{}, dumpPath string) error {
	desired := []struct {
		Nome string
		Tipo string
	}{
		{"material_kind", "TEXT"},
		{"pricing_model", "TEXT"},
		{"service_type", "TEXT"},
		{"units", "INTEGER"},
		{"weight", "DECIMAL"},
		{"rn_day", "INTEGER"},
		{"first_with_freight", "INTEGER"},
		{"rn_month", "INTEGER"},
		{"has_additional_month", "BOOLEAN"},
		{"charge_freight_today", "BOOLEAN"},
	}

	// dump (sempre: lista desejada)
	if dumpPath != "" {
		var b strings.Builder
		b.WriteString(`API_BASE=${API_BASE:-"`)
		b.WriteString(strings.TrimRight(apiBase, "/"))
		b.WriteString(`"}` + "\n\n")
		for _, d := range desired {
			payload := map[string]any{"nome": d.Nome, "tipo_dado": d.Tipo}
			j, _ := json.Marshal(payload)
			b.WriteString("curl -sS -X POST \"${API_BASE}/variavel/\" -H 'Content-Type: application/json' -d @- <<'JSON'\n")
			b.WriteString(string(j))
			b.WriteString("\nJSON\n\n")
		}
		_ = os.WriteFile(dumpPath, []byte(b.String()), 0o644)
	}

	vars, err := listVariables(client, apiBase, retries, sem)
	if err != nil {
		return err
	}
	exists := map[string]bool{}
	for _, v := range vars {
		exists[strings.ToLower(v.Nome)] = true
	}
	for _, d := range desired {
		if exists[strings.ToLower(d.Nome)] {
			continue
		}
		if err := createVariable(client, apiBase, d.Nome, d.Tipo, retries, sem); err != nil {
			return err
		}
	}
	return nil
}

// ================= Parser Regras =================

type RuleSources struct {
	Pricings     map[RuleKey]PricingSpec
	GenericFrete map[RuleKey]float64 // material "" = coringa por (estab, service_type, pricing_model)
	MonthlyByEID map[string]MonthlySpec
	Estabs       []string
}

func readRulesPricingCSV(path string) (*RuleSources, error) {
	comma, header, rows, err := readCSV(path)
	_ = comma
	if err != nil {
		return nil, err
	}
	h := headerIndex(header)

	pMap := make(map[RuleKey]PricingSpec)
	genFrete := make(map[RuleKey]float64)
	monthly := make(map[string]MonthlySpec)

	addOrReplace := func(spec PricingSpec) {
		old, ok := pMap[spec.Key]
		if !ok || spec.TS.After(old.TS) {
			pMap[spec.Key] = spec
		}
	}

	for _, row := range rows {
		eid := strings.TrimSpace(getAny(row, h, "establishment_external_id"))
		if eid == "" {
			continue
		}
		ativo := upperTrim(getAny(row, h, "pricing_ativo"))
		if ativo != "" && ativo != "TRUE" && ativo != "1" && ativo != "YES" && ativo != "T" && ativo != "SIM" {
			continue
		}

		mat := upperTrim(getAny(row, h, "material_kind_normalizado", "material_kind"))
		if mat == "-" {
			mat = ""
		}
		svc := upperTrim(getAny(row, h, "service_type"))
		pm := upperTrim(getAny(row, h, "pricing_model"))

		created := parseTS(getAny(row, h, "created_at"))
		updated := parseTS(getAny(row, h, "updated_at"))
		ts := created
		if !updated.IsZero() && updated.After(ts) {
			ts = updated
		}

		unitPrice := parseNumberLoose(getAny(row, h, "unit_price"))
		minCollect := parseNumberLoose(getAny(row, h, "min_collect_fee"))
		minQty := parseNumberLoose(getAny(row, h, "min_quantity_price"))
		frete := parseNumberLoose(getAny(row, h, "freight_fee"))

		// mensalidade
		if pm == "FIXED_AMOUNT" {
			ms := monthly[eid]
			if !ms.Has || unitPrice > ms.Fee || ts.After(ms.TS) {
				if unitPrice >= ms.Fee {
					ms.Fee = unitPrice
					ms.TS = ts
					ms.Has = true
				}
			}
			if svc == "ADDITIONAL" {
				ms.IsAdditional = true
			}
			monthly[eid] = ms
			continue
		}

		// se material vazio e existir frete: guarda também como "coringa" por (estab, service_type, pricing_model)
		if mat == "" && frete > 0 {
			genFrete[RuleKey{Estab: eid, Material: "", ServiceType: svc, PricingModel: pm}] = frete
			// NÃO dá continue: também precisamos criar PricingSpec coringa para registrar regra na API
		}

		key := RuleKey{Estab: eid, Material: mat, ServiceType: svc, PricingModel: pm}
		addOrReplace(PricingSpec{
			Key:           key,
			UnitPrice:     unitPrice,
			MinCollectFee: minCollect,
			MinQty:        minQty,
			FreightFee:    frete,
			TS:            ts,
		})
	}

	// lista de estabs
	set := map[string]struct{}{}
	for k := range pMap {
		set[k.Estab] = struct{}{}
	}
	for eid := range monthly {
		set[eid] = struct{}{}
	}
	for k := range genFrete {
		set[k.Estab] = struct{}{}
	}
	estabs := make([]string, 0, len(set))
	for eid := range set {
		estabs = append(estabs, eid)
	}
	sort.Strings(estabs)

	return &RuleSources{
		Pricings:     pMap,
		GenericFrete: genFrete,
		MonthlyByEID: monthly,
		Estabs:       estabs,
	}, nil
}

func isUnitsFamily(pm string) bool {
	switch pm {
	case "UNITS", "UNITS_COLLECT_FEE", "GROUPS", "GROUPS_COLLECT_FEE":
		return true
	default:
		return false
	}
}
func isWeightFamily(pm string) bool {
	switch pm {
	case "WEIGHT", "WEIGHT_COLLECT_FEE", "VARIABLE_VALUE":
		return true
	default:
		return false
	}
}

func buildFormula(spec PricingSpec, monthly MonthlySpec, genericFrete map[RuleKey]float64) string {
	f6 := func(v float64) string { return fmt.Sprintf("%.6f", v) }
	fee := f6(spec.UnitPrice)

	var base string
	switch spec.Key.PricingModel {
	case "GROUPS_COLLECT_FEE", "GROUPS":
		// groups não tem componente por units/weight; normalmente é só frete (e raramente min_collect_fee)
		base = "0"
	default:
		if isUnitsFamily(spec.Key.PricingModel) {
			base = fmt.Sprintf("(%s) * units", fee)
		} else if isWeightFamily(spec.Key.PricingModel) {
			base = fmt.Sprintf("(%s) * weight", fee)
		} else {
			base = fmt.Sprintf("(%s) * units", fee)
		}
	}

	parts := []string{base}

	// min_collect_fee é aditivo
	if spec.MinCollectFee > 0 {
		parts = append(parts, fmt.Sprintf("(%s)", f6(spec.MinCollectFee)))
	}

	// min_quantity_price como “complemento até o mínimo”
	if spec.MinQty > 0 {
		min := f6(spec.MinQty)
		if isUnitsFamily(spec.Key.PricingModel) {
			parts = append(parts,
				fmt.Sprintf("((%s) * ((%s) - units) if ((%s) - units) > 0 else 0)", fee, min, min),
			)
		} else if isWeightFamily(spec.Key.PricingModel) {
			parts = append(parts,
				fmt.Sprintf("((%s) * ((%s) - weight) if ((%s) - weight) > 0 else 0)", fee, min, min),
			)
		}
	}

	// frete: respeita material específico; senão usa frete coringa (material vazio)
	frete := spec.FreightFee
	if frete <= 0 {
		if g, ok := genericFrete[RuleKey{Estab: spec.Key.Estab, Material: "", ServiceType: spec.Key.ServiceType, PricingModel: spec.Key.PricingModel}]; ok {
			frete = g
		}
	}
	if frete > 0 {
		parts = append(parts,
			fmt.Sprintf("((%s) if charge_freight_today else 0)", f6(frete)),
		)
	}

	// mensalidade: 1x no mês (rn_month==1); se mensalidade “additional”, só cobra se has_additional_month
	if monthly.Has && monthly.Fee > 0 {
		isAdd := "False"
		if monthly.IsAdditional {
			isAdd = "True"
		}
		parts = append(parts,
			fmt.Sprintf("((%.6f) if (rn_month == 1 and ((not %s) or has_additional_month)) else 0)", monthly.Fee, isAdd),
		)
	}

	return strings.Join(parts, " + ")
}

func buildCondition(spec PricingSpec) string {
	// match exato; se material == "" => regra coringa (qualquer material) mas ainda exige pricing_model + service_type
	if spec.Key.Material == "" {
		return fmt.Sprintf("pricing_model == '%s' and service_type == '%s'",
			spec.Key.PricingModel, spec.Key.ServiceType)
	}
	return fmt.Sprintf("material_kind == '%s' and pricing_model == '%s' and service_type == '%s'",
		spec.Key.Material, spec.Key.PricingModel, spec.Key.ServiceType)
}

type ruleReq struct {
	Estab   string
	Cond    string
	Formula string
	Prio    int
}

func dumpRulesRequests(apiBase, path string, reqs []ruleReq) {
	if path == "" {
		return
	}
	var b strings.Builder
	b.WriteString(`API_BASE=${API_BASE:-"`)
	b.WriteString(strings.TrimRight(apiBase, "/"))
	b.WriteString(`"}` + "\n\n")
	for _, rr := range reqs {
		payload := map[string]any{
			"establishment_id": rr.Estab,
			"condicao":         rr.Cond,
			"formula":          rr.Formula,
			"prioridade":       rr.Prio,
		}
		j, _ := json.Marshal(payload)
		b.WriteString("curl -sS -X POST \"${API_BASE}/regra/\" -H 'Content-Type: application/json' -d @- <<'JSON'\n")
		b.WriteString(string(j))
		b.WriteString("\nJSON\n\n")
	}
	_ = os.WriteFile(path, []byte(b.String()), 0o644)
}

func ensureRules(client *http.Client, apiBase string, rules *RuleSources, prioBase int, retries int, sem chan struct{}, dumpPath string) error {
	byEstab := map[string][]PricingSpec{}
	for _, spec := range rules.Pricings {
		byEstab[spec.Key.Estab] = append(byEstab[spec.Key.Estab], spec)
	}
	for eid := range rules.MonthlyByEID {
		if _, ok := byEstab[eid]; !ok {
			byEstab[eid] = nil
		}
	}

	allDesiredForDump := make([]ruleReq, 0, 20000)

	for eid, specs := range byEstab {
		existing, err := listRulesByEstab(client, apiBase, eid, retries, sem)
		if err != nil {
			return fmt.Errorf("listRules estab %s: %w", eid, err)
		}
		existsSig := map[string]bool{}
		for _, r := range existing {
			existsSig[r.Condicao+"||"+r.Formula] = true
		}

		sort.Slice(specs, func(i, j int) bool {
			a, b := specs[i], specs[j]
			if a.Key.ServiceType != b.Key.ServiceType {
				return a.Key.ServiceType < b.Key.ServiceType
			}
			if a.Key.PricingModel != b.Key.PricingModel {
				return a.Key.PricingModel < b.Key.PricingModel
			}
			// IMPORTANT: material "" vem antes => prioridade menor => perde para material específico (prioridade maior)
			return a.Key.Material < b.Key.Material
		})

		monthly := rules.MonthlyByEID[eid]
		nextPrio := prioBase

		// (Opcional, mas útil): regra "fallback mensal" que casa sempre,
		// para casos onde a 1ª coleta do mês não casa com nenhuma trinca, mas existe mensalidade.
		// Prioridade menor que as outras: não deve ganhar quando houver regra específica.
		if monthly.Has && monthly.Fee > 0 {
			isAdd := "False"
			if monthly.IsAdditional {
				isAdd = "True"
			}
			cond := "True"
			form := fmt.Sprintf("((%.6f) if (rn_month == 1 and ((not %s) or has_additional_month)) else 0)", monthly.Fee, isAdd)

			fallbackPrio := prioBase - 1
			if fallbackPrio < 1 {
				fallbackPrio = 1
			}

			allDesiredForDump = append(allDesiredForDump, ruleReq{Estab: eid, Cond: cond, Formula: form, Prio: fallbackPrio})

			sig := cond + "||" + form
			if !existsSig[sig] {
				if err := createRule(client, apiBase, eid, cond, form, fallbackPrio, retries, sem); err != nil {
					return fmt.Errorf("create fallback monthly rule estab %s: %w", eid, err)
				}
				existsSig[sig] = true
			}
		}

		for _, spec := range specs {
			cond := buildCondition(spec)
			form := buildFormula(spec, monthly, rules.GenericFrete)
			sig := cond + "||" + form

			allDesiredForDump = append(allDesiredForDump, ruleReq{Estab: eid, Cond: cond, Formula: form, Prio: nextPrio})

			if existsSig[sig] {
				nextPrio++
				continue
			}
			if err := createRule(client, apiBase, eid, cond, form, nextPrio, retries, sem); err != nil {
				return fmt.Errorf("create rule estab %s (%s): %w", eid, cond, err)
			}
			existsSig[sig] = true
			nextPrio++
		}
	}

	// dump das requisições "necessárias"
	if dumpPath != "" {
		dumpRulesRequests(apiBase, dumpPath, allDesiredForDump)
	}

	return nil
}

// ================= Preprocess coletas -> contexto (sem calcular faturamento) =================

type RowCtx struct {
	estab  string
	day    string
	month  string
	mk     string
	st     string
	pm     string
	units  int
	weight float64

	rnDay           int
	firstWithFrete  int
	rnMonth         int
	hasAdditionalMo bool
}

func monthKey(day string) string {
	day = strings.TrimSpace(day)
	if len(day) >= 7 {
		return day[:7]
	}
	return ""
}

func buildRowContexts(rows [][]string, h map[string]int, rules *RuleSources) []RowCtx {
	out := make([]RowCtx, len(rows))

	hasAdd := map[string]bool{}
	for _, row := range rows {
		eid := strings.TrimSpace(get(row, h, "establishment_external_id"))
		day := strings.TrimSpace(get(row, h, "service_day"))
		st := upperTrim(get(row, h, "service_type_exibido"))
		mo := monthKey(day)
		key := eid + "|" + mo
		if st == "ADDITIONAL" {
			hasAdd[key] = true
		}
	}

	rnDay := map[string]int{}
	rnMonth := map[string]int{}
	hasFrete := make([]bool, len(rows))

	lookupFrete := func(eid, mk, st, pm string) float64 {
		key := RuleKey{Estab: eid, Material: mk, ServiceType: st, PricingModel: pm}
		if spec, ok := rules.Pricings[key]; ok && spec.FreightFee > 0 {
			return spec.FreightFee
		}
		// coringa material vazio
		if g, ok := rules.GenericFrete[RuleKey{Estab: eid, Material: "", ServiceType: st, PricingModel: pm}]; ok {
			return g
		}
		// também pode existir PricingSpec coringa (Material == "")
		if spec, ok := rules.Pricings[RuleKey{Estab: eid, Material: "", ServiceType: st, PricingModel: pm}]; ok && spec.FreightFee > 0 {
			return spec.FreightFee
		}
		return 0
	}

	for i, row := range rows {
		eid := strings.TrimSpace(get(row, h, "establishment_external_id"))
		day := strings.TrimSpace(get(row, h, "service_day"))
		mo := monthKey(day)

		mk := upperTrim(get(row, h, "kind_of_material"))
		st := upperTrim(get(row, h, "service_type_exibido"))
		pm := upperTrim(get(row, h, "pricing_model_exibido"))

		units := parseIntLoose(get(row, h, "units"))
		weight := parseNumberLoose(get(row, h, "weight"))

		kd := eid + "|" + day
		rnDay[kd]++
		rd := rnDay[kd]

		km := eid + "|" + mo
		rnMonth[km]++
		rm := rnMonth[km]

		ham := hasAdd[km]

		frete := lookupFrete(eid, mk, st, pm)
		hasFrete[i] = frete > 0

		out[i] = RowCtx{
			estab:           eid,
			day:             day,
			month:           mo,
			mk:              mk,
			st:              st,
			pm:              pm,
			units:           units,
			weight:          weight,
			rnDay:           rd,
			rnMonth:         rm,
			hasAdditionalMo: ham,
			firstWithFrete:  0,
		}
	}

	first := map[string]int{}
	for i := range out {
		k := out[i].estab + "|" + out[i].day
		if hasFrete[i] {
			if _, ok := first[k]; !ok {
				first[k] = out[i].rnDay
			}
		}
	}
	for i := range out {
		k := out[i].estab + "|" + out[i].day
		out[i].firstWithFrete = first[k]
	}

	return out
}

// ================= Exec por linha =================

func processRow(client *http.Client, apiBase string, row []string, h map[string]int, ctx RowCtx, retries int, sem chan struct{}) ([]string, *APIError) {
	eid := ctx.estab
	if eid == "" {
		return nil, &APIError{Detail: "sem establishment_external_id", HTTPStatus: 0}
	}

	hauler := strings.TrimSpace(get(row, h, "hauler_name"))
	evento := fmt.Sprintf("%s|%s|%s|%s|%s|%s|rd%d|rm%d",
		ctx.day, eid, hauler, ctx.mk, ctx.st, ctx.pm, ctx.rnDay, ctx.rnMonth,
	)

	chargeToday := false
	if ctx.firstWithFrete > 0 && ctx.rnDay == ctx.firstWithFrete {
		chargeToday = true
	}

	reqCtx := map[string]any{
		"establishment_id":     eid,
		"evento_external_id":   evento,
		"material_kind":        ctx.mk,
		"pricing_model":        ctx.pm,
		"service_type":         ctx.st,
		"units":                ctx.units,
		"weight":               ctx.weight,
		"rn_day":               ctx.rnDay,
		"first_with_freight":   ctx.firstWithFrete,
		"rn_month":             ctx.rnMonth,
		"has_additional_month": ctx.hasAdditionalMo,
		"charge_freight_today": chargeToday,
	}

	ok, apiErr := simulate(client, apiBase, reqCtx, retries, sem)
	valFinal := 0.0
	if ok != nil {
		valFinal = ok.Valor
	}

	outHeader := []string{
		"mes_fatura",
		"establishment_external_id",
		"nome_grupo_pai",
		"generator_name",
		"establishment_name",
		"type_of_establishment",
		"service_day",
		"hauler_name",
		"kind_of_material",
		"service_type_exibido",
		"pricing_model_exibido",
		"units",
		"weight",
		"valor_unitario",
		"valor_kg",
		"valor_servico_itens",
		"valor_servico_kg",
		"min_collect_fee",
		"valor_frete",
		"valor_fixo",
		"preco_minimo",
		"valor_final",
	}

	out := make([]string, len(outHeader))
	for i, col := range outHeader {
		switch col {
		case "valor_final":
			out[i] = formatBR2(valFinal)
		default:
			out[i] = get(row, h, col)
		}
	}
	return out, apiErr
}

// ================= Main =================

func main() {
	apiBase := flag.String("api", "http://127.0.0.1:8000", "Base URL da API")
	rulesPath := flag.String("rules", "", "CSV de regras (regras_pricing.csv)")
	coletasPath := flag.String("coletas", "", "CSV de coletas/entrada (entrada.csv)")
	outPath := flag.String("out", "saida.csv", "CSV de saída")

	bootstrap := flag.Bool("bootstrap", false, "Cria variáveis e regras (idempotente por condicao+formula)")
	prioBase := flag.Int("prio-base", 50000, "Prioridade base para regras geradas")
	workers := flag.Int("workers", max(4, runtime.NumCPU()), "Workers (paralelismo por linha)")
	maxInflight := flag.Int("max-inflight", 200, "Máximo de requisições HTTP simultâneas (global)")
	retries := flag.Int("retries", 2, "Retries por requisição em 429/5xx")
	timeout := flag.Duration("timeout", 25*time.Second, "Timeout por requisição HTTP")

	dumpVars := flag.String("dump-vars", "variaveis_requests.txt", "Arquivo .txt com requests para criar variáveis (somente no bootstrap)")
	dumpRules := flag.String("dump-rules", "regras_requests.txt", "Arquivo .txt com requests para criar regras (somente no bootstrap)")

	flag.Parse()

	if *rulesPath == "" {
		fmt.Fprintln(os.Stderr, "erro: informe -rules (regras_pricing.csv)")
		os.Exit(1)
	}
	if *coletasPath == "" {
		fmt.Fprintln(os.Stderr, "erro: informe -coletas (entrada.csv)")
		os.Exit(1)
	}

	client := newHTTPClient(*timeout)
	sem := make(chan struct{}, *maxInflight)

	rules, err := readRulesPricingCSV(*rulesPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "erro lendo rules:", err)
		os.Exit(1)
	}

	if *bootstrap {
		varDump := ""
		ruleDump := ""
		if *dumpVars != "" {
			varDump = *dumpVars
		}
		if *dumpRules != "" {
			ruleDump = *dumpRules
		}

		if err := ensureVariables(client, *apiBase, *retries, sem, varDump); err != nil {
			fmt.Fprintln(os.Stderr, "erro garantindo variáveis:", err)
			os.Exit(1)
		}
		if err := ensureRules(client, *apiBase, rules, *prioBase, *retries, sem, ruleDump); err != nil {
			fmt.Fprintln(os.Stderr, "erro no bootstrap de regras:", err)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, "Bootstrap OK (variáveis + regras). Dumps:", *dumpVars, "e", *dumpRules)
	}

	comma, header, rows, err := readCSV(*coletasPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "erro lendo coletas:", err)
		os.Exit(1)
	}
	h := headerIndex(header)

	rowCtxs := buildRowContexts(rows, h, rules)

	outHeader := []string{
		"mes_fatura",
		"establishment_external_id",
		"nome_grupo_pai",
		"generator_name",
		"establishment_name",
		"type_of_establishment",
		"service_day",
		"hauler_name",
		"kind_of_material",
		"service_type_exibido",
		"pricing_model_exibido",
		"units",
		"weight",
		"valor_unitario",
		"valor_kg",
		"valor_servico_itens",
		"valor_servico_kg",
		"min_collect_fee",
		"valor_frete",
		"valor_fixo",
		"preco_minimo",
		"valor_final",
	}

	outRows := make([][]string, len(rows)+1)
	outRows[0] = outHeader

	jobs := make(chan Job, *workers*2)
	results := make(chan Result, *workers*2)

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				out, apiErr := processRow(client, *apiBase, job.Row, h, rowCtxs[job.Idx], *retries, sem)
				var err error
				if apiErr != nil {
					msg := apiErr.Error
					if msg == "" {
						msg = apiErr.Detail
					}
					if msg == "" {
						msg = "erro API"
					}
					err = fmt.Errorf("status %d: %s", apiErr.HTTPStatus, msg)
				}
				results <- Result{Idx: job.Idx, Out: out, Err: err}
			}
		}()
	}

	go func() {
		for i := range rows {
			jobs <- Job{Idx: i, Row: rows[i]}
		}
		close(jobs)
	}()

	processed := 0
	errCount := 0
	lastPrint := time.Now()
	printProgress := func(force bool) {
		if !force && time.Since(lastPrint) < 750*time.Millisecond {
			return
		}
		pct := 0.0
		if len(rows) > 0 {
			pct = (float64(processed) / float64(len(rows))) * 100
		}
		fmt.Fprintf(os.Stderr, "\rProcessados: %d/%d (%.1f%%) | erros: %d", processed, len(rows), pct, errCount)
		lastPrint = time.Now()
	}

	errAgg := map[string]int{}
	var firstErr error

	go func() {
		wg.Wait()
		close(results)
	}()

	for res := range results {
		outRows[res.Idx+1] = res.Out
		processed++

		if res.Err != nil {
			errCount++
			s := res.Err.Error()
			if strings.Contains(s, "status 404") {
				errAgg["status 404"]++
			} else if strings.Contains(s, "status 422") {
				errAgg["status 422"]++
			} else {
				errAgg["outros"]++
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("linha %d: %w", res.Idx+2, res.Err)
			}
		}

		printProgress(processed == len(rows))
	}
	fmt.Fprintln(os.Stderr)

	if firstErr != nil {
		fmt.Fprintln(os.Stderr, "processamento terminou com erros (primeiro):", firstErr)
		fmt.Fprintln(os.Stderr, "\nResumo de erros:")
		type kv struct {
			k string
			v int
		}
		var vs []kv
		for k, v := range errAgg {
			vs = append(vs, kv{k, v})
		}
		sort.Slice(vs, func(i, j int) bool { return vs[i].v > vs[j].v })
		for i := 0; i < len(vs) && i < 10; i++ {
			fmt.Fprintf(os.Stderr, "- %dx %s\n", vs[i].v, vs[i].k)
		}
	}

	if err := writeCSV(*outPath, comma, outRows); err != nil {
		fmt.Fprintln(os.Stderr, "erro escrevendo output:", err)
		os.Exit(1)
	}

	fmt.Println("OK:", *outPath)
}
