package cajero

import (
	"errors"
	"strings"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// T2.8 · el parseo de la lista de CPUs del kernel
// ─────────────────────────────────────────────────────────────────────────────

// TestParsearListaCPUs recorre los formatos que /proc emite de verdad y la basura que podría llegar si
// la lectura sale mal. El caso que más importa es la CADENA VACÍA: si colara como conjunto vacío, la
// comparación diría «disjuntos» —el veredicto tranquilizador— justo cuando no se sabe nada.
func TestParsearListaCPUs(t *testing.T) {
	casos := []struct {
		nombre   string
		entrada  string
		esperado []int // nil ⇒ se espera error
	}{
		{"rango", "0-3", []int{0, 1, 2, 3}},
		{"sueltas", "2,4", []int{2, 4}},
		{"mixto", "0-1,6", []int{0, 1, 6}},
		{"una sola", "3", []int{3}},
		{"rango de uno", "5-5", []int{5}},
		{"con espacios alrededor", " 0-1 , 4 ", []int{0, 1, 4}},
		{"duplicados solapados", "0-2,1-3", []int{0, 1, 2, 3}},
		{"vacía", "", nil},
		{"sólo espacios", "   ", nil},
		{"basura", "basura", nil},
		{"rango abierto por la derecha", "1-", nil},
		{"rango abierto por la izquierda", "-1", nil},
		{"rango hacia atrás", "3-1", nil},
		{"coma colgando", "0,", nil},
		{"coma doble", "0,,2", nil},
		{"negativo explícito", "0,-2", nil},
		{"decimal", "1.5", nil},
		{"fuera de rango", "0-999999999", nil},
		// Los dos lados del techo de maxCPUsPlausibles. El índice exacto se acepta —una máquina con
		// CONFIG_NR_CPUS al máximo es rara pero legítima— y el siguiente ya no.
		{"justo en el techo", "8192", []int{8192}},
		{"un paso por encima del techo", "8193", nil},
		{"techo superado dentro de un rango", "0-8193", nil},
	}

	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			conjunto, err := parsearListaCPUs(c.entrada)
			if c.esperado == nil {
				if err == nil {
					t.Fatalf("parsearListaCPUs(%q) = %v, se esperaba ERROR", c.entrada, conjunto)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsearListaCPUs(%q): %v", c.entrada, err)
			}
			if len(conjunto) != len(c.esperado) {
				t.Fatalf("parsearListaCPUs(%q) = %q, se esperaban %d CPUs", c.entrada, conjunto.String(), len(c.esperado))
			}
			for _, cpu := range c.esperado {
				if _, ok := conjunto[cpu]; !ok {
					t.Errorf("parsearListaCPUs(%q) = %q: falta la CPU %d", c.entrada, conjunto.String(), cpu)
				}
			}
		})
	}
}

// TestConjuntoCPUString: la lista sale ORDENADA, que es lo que hace comparable la línea de log con lo
// que el operador escribió en su `taskset`. El recorrido de un map en Go no lo está.
func TestConjuntoCPUString(t *testing.T) {
	conjunto, err := parsearListaCPUs("6,0-1,4")
	if err != nil {
		t.Fatalf("parsearListaCPUs: %v", err)
	}
	if got := conjunto.String(); got != "0,1,4,6" {
		t.Errorf("String() = %q, se esperaba \"0,1,4,6\"", got)
	}
	vacio := conjuntoCPU{}
	if got := vacio.String(); got != "" {
		t.Errorf("String() del conjunto vacío = %q, se esperaba \"\"", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// T2.8 · las dos operaciones de conjuntos que sostienen el veredicto
// ─────────────────────────────────────────────────────────────────────────────

// TestInterseccion: es LA operación de la que depende todo el chequeo — «se pisan» es exactamente «la
// intersección no está vacía». Se prueba en LOS DOS ÓRDENES porque la implementación recorre el conjunto
// más pequeño para no pagar el coste del mayor, y ese intercambio es justo donde se cuela un bug
// asimétrico que devolvería vacío según quién llame a quién.
func TestInterseccion(t *testing.T) {
	casos := []struct {
		nombre   string
		a, b     string
		esperado string
	}{
		{"disjuntos contiguos (B2b)", "0-3", "4-5", ""},
		{"disjuntos entrelazados", "0,2,4", "1,3,5", ""},
		{"una sola CPU compartida", "0-3", "3-5", "3"},
		{"idénticos", "0-3", "0-3", "0,1,2,3"},
		{"uno contiene al otro", "0-5", "4-5", "4,5"},
		{"tamaños muy distintos", "0-5", "5", "5"},
	}

	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			a, err := parsearListaCPUs(c.a)
			if err != nil {
				t.Fatalf("parsearListaCPUs(%q): %v", c.a, err)
			}
			b, err := parsearListaCPUs(c.b)
			if err != nil {
				t.Fatalf("parsearListaCPUs(%q): %v", c.b, err)
			}
			if got := a.interseccion(b).String(); got != c.esperado {
				t.Errorf("%q ∩ %q = %q, se esperaba %q", c.a, c.b, got, c.esperado)
			}
			// CONMUTATIVA: quién pregunta no puede cambiar la respuesta.
			if got := b.interseccion(a).String(); got != c.esperado {
				t.Errorf("%q ∩ %q = %q, se esperaba %q (la intersección debe ser conmutativa)", c.b, c.a, got, c.esperado)
			}
		})
	}
}

// TestContieneTodas es la que separa «el cajero está confinado a un trozo» de «al cajero no le pusieron
// taskset»: el segundo caso es el escenario G2 medido, y se detecta porque el conjunto del cajero cubre
// entero el censo de la máquina.
func TestContieneTodas(t *testing.T) {
	casos := []struct {
		nombre    string
		conjunto  string
		otro      string
		contieneT bool
	}{
		{"el cajero cubre la máquina entera (G2)", "0-5", "0-5", true},
		{"el cajero cubre de sobra", "0-7", "0-5", true},
		{"le falta una CPU", "0-4", "0-5", false},
		{"le falta una del medio", "0,1,3,4,5", "0-5", false},
		{"trozo disjunto", "0-3", "4-5", false},
		{"una CPU cubre una máquina de una CPU", "0", "0", true},
	}

	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			conjunto, err := parsearListaCPUs(c.conjunto)
			if err != nil {
				t.Fatalf("parsearListaCPUs(%q): %v", c.conjunto, err)
			}
			otro, err := parsearListaCPUs(c.otro)
			if err != nil {
				t.Fatalf("parsearListaCPUs(%q): %v", c.otro, err)
			}
			if got := conjunto.contieneTodas(otro); got != c.contieneT {
				t.Errorf("%q.contieneTodas(%q) = %v, se esperaba %v", c.conjunto, c.otro, got, c.contieneT)
			}
		})
	}

	// NO es simétrica, y confundirlo invertiría el veredicto de G2: un cajero confinado a 0-3 en una
	// máquina de 6 no «contiene» el censo, pero el censo sí lo contiene a él.
	cajero, err := parsearListaCPUs("0-3")
	if err != nil {
		t.Fatalf("parsearListaCPUs: %v", err)
	}
	maquina, err := parsearListaCPUs("0-5")
	if err != nil {
		t.Fatalf("parsearListaCPUs: %v", err)
	}
	if cajero.contieneTodas(maquina) {
		t.Error("un cajero confinado a 0-3 NO cubre una máquina de 6 CPUs")
	}
	if !maquina.contieneTodas(cajero) {
		t.Error("una máquina de 6 CPUs SÍ cubre un cajero confinado a 0-3")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// T2.8 · los tres veredictos del reparto
// ─────────────────────────────────────────────────────────────────────────────

// TestClasificarReparto cubre los tres casos que la medición separa, y el orden entre dos de ellos: un
// cajero suelto en toda la máquina TAMBIÉN solapa, así que si «sin confinar» no se mirara primero nunca
// se distinguiría y el operador recibiría el consejo equivocado (mover al vecino, que ya estaba bien).
func TestClasificarReparto(t *testing.T) {
	casos := []struct {
		nombre    string
		ollama    string
		cajero    string
		presentes string
		veredicto veredictoAfinidad
		comunes   string
	}{
		{
			nombre: "B2b · vecino en 4-5 y cajero en 0-3 (el 0 % medido)",
			ollama: "4-5", cajero: "0-3", presentes: "0-5",
			veredicto: afinidadDisjunta, comunes: "",
		},
		{
			nombre: "B2a · aislado 5/1",
			ollama: "0-4", cajero: "5", presentes: "0-5",
			veredicto: afinidadDisjunta, comunes: "",
		},
		{
			nombre: "G2 · vecino confinado a 2 vCPU pero el cajero LIBRE en las 6 (el 17,2 % medido)",
			ollama: "4-5", cajero: "0-5", presentes: "0-5",
			veredicto: afinidadCajeroSinConfinar, comunes: "4,5",
		},
		{
			nombre: "solapamiento PARCIAL: los dos confinados, pero comparten una CPU",
			ollama: "3-5", cajero: "0-3", presentes: "0-5",
			veredicto: afinidadSolapada, comunes: "3",
		},
		{
			nombre: "solapamiento TOTAL: los dos en el mismo trozo",
			ollama: "0-3", cajero: "0-3", presentes: "0-5",
			veredicto: afinidadSolapada, comunes: "0,1,2,3",
		},
		{
			nombre: "el cajero cubre a Ollama y algo más, pero no la máquina entera",
			ollama: "4", cajero: "0-4", presentes: "0-5",
			veredicto: afinidadSolapada, comunes: "4",
		},
		{
			nombre: "sin censo de la máquina: el cajero suelto degrada a «solapan», que lleva el mismo Warn",
			ollama: "4-5", cajero: "0-5", presentes: "",
			veredicto: afinidadSolapada, comunes: "4,5",
		},
		{
			nombre: "censo ilegible: se ignora, no rompe la comparación",
			ollama: "4-5", cajero: "0-3", presentes: "basura",
			veredicto: afinidadDisjunta, comunes: "",
		},
		{
			nombre: "máquina de una sola CPU: no hay aislamiento posible y se dice",
			ollama: "0", cajero: "0", presentes: "0",
			veredicto: afinidadCajeroSinConfinar, comunes: "0",
		},
	}

	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			reparto, err := clasificarReparto(c.ollama, c.cajero, c.presentes)
			if err != nil {
				t.Fatalf("clasificarReparto(%q, %q, %q): %v", c.ollama, c.cajero, c.presentes, err)
			}
			if reparto.Veredicto != c.veredicto {
				t.Errorf("veredicto = %q, se esperaba %q (ollama=%q cajero=%q)",
					reparto.Veredicto, c.veredicto, c.ollama, c.cajero)
			}
			if got := reparto.Comunes.String(); got != c.comunes {
				t.Errorf("cpus compartidas = %q, se esperaba %q", got, c.comunes)
			}
			// La invariante que ata las dos cosas: sólo el veredicto disjunto puede tener el
			// solapamiento vacío. Sin esto, un bug que dijera «disjunto» con CPUs comunes pasaría.
			if (reparto.Veredicto == afinidadDisjunta) != (len(reparto.Comunes) == 0) {
				t.Errorf("veredicto %q con %d CPUs compartidas: disjunto ⇔ sin CPUs comunes",
					reparto.Veredicto, len(reparto.Comunes))
			}
		})
	}
}

// TestClasificarReparto_LecturaIlegibleEsError: una afinidad que no se puede leer NO puede acabar en un
// veredicto. Es el mismo motivo por el que la cadena vacía se rechaza en el parser: el fallo tiene que
// llegar al llamante como error para que salga por Warn, no convertirse en «disjuntos».
func TestClasificarReparto_LecturaIlegibleEsError(t *testing.T) {
	casos := []struct{ nombre, ollama, cajero string }{
		{"ollama vacío", "", "0-3"},
		{"cajero vacío", "0-3", ""},
		{"ollama basura", "no-soy-una-lista", "0-3"},
		{"cajero basura", "0-3", "no-soy-una-lista"},
		{"los dos vacíos", "", ""},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			reparto, err := clasificarReparto(c.ollama, c.cajero, "0-5")
			if err == nil {
				t.Fatalf("clasificarReparto(%q, %q) = %q, se esperaba ERROR", c.ollama, c.cajero, reparto.Veredicto)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// T2.8 · lo que el arranque LOGUEA con cada veredicto
// ─────────────────────────────────────────────────────────────────────────────

// TestRegistrarReparto_NivelYReceta: la configuración buena es Info y las dos malas son Warn, y las dos
// malas llevan la receta medida. Que el nivel esté bien no es cosmético: un Warn degradado a Info es
// exactamente el aviso que nadie ve.
func TestRegistrarReparto_NivelYReceta(t *testing.T) {
	casos := []struct {
		nombre    string
		lec       lecturaAfinidad
		nivel     string
		fragmento string
		conReceta bool
	}{
		{
			nombre:    "disjuntos ⇒ Info",
			lec:       lecturaAfinidad{Ollama: "4-5", Cajero: "0-3", Presentes: "0-5"},
			nivel:     "info",
			fragmento: "DISJUNTO",
		},
		{
			nombre:    "solapan ⇒ Warn con la receta",
			lec:       lecturaAfinidad{Ollama: "3-5", Cajero: "0-3", Presentes: "0-5"},
			nivel:     "warn",
			fragmento: "SE PISAN",
			conReceta: true,
		},
		{
			nombre:    "cajero sin confinar (G2) ⇒ Warn con la receta",
			lec:       lecturaAfinidad{Ollama: "4-5", Cajero: "0-5", Presentes: "0-5"},
			nivel:     "warn",
			fragmento: "NO está confinado",
			conReceta: true,
		},
	}

	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			log := &logCaptura{}
			cj := &Cajero{log: log, numThread: 1, ollamaURL: "http://127.0.0.1:11434"}
			cj.registrarReparto(c.lec)

			e, ok := log.buscar(c.fragmento)
			if !ok {
				t.Fatalf("no se logueó el veredicto esperado (%q); se logueó:\n%s", c.fragmento, log.texto())
			}
			if e.nivel != c.nivel {
				t.Errorf("nivel = %q, se esperaba %q para %q", e.nivel, c.nivel, c.fragmento)
			}
			if c.conReceta && !strings.Contains(e.msg, recetaSolapamiento) {
				t.Errorf("el aviso de %q no lleva la receta medida; msg = %q", c.fragmento, e.msg)
			}
			if !c.conReceta && strings.Contains(e.msg, recetaSolapamiento) {
				t.Errorf("la configuración BUENA no debe llevar la receta de arreglo; msg = %q", e.msg)
			}
			// Los dos repartos viajan siempre en la línea: la conclusión sin el dato no se puede auditar.
			if todo := log.texto(); !strings.Contains(todo, "cpus_ollama") || !strings.Contains(todo, "cpus_cajero") {
				t.Errorf("la línea debe llevar los dos repartos; se logueó:\n%s", todo)
			}
		})
	}
}

// TestRegistrarReparto_NuncaEsFatal_ConLecturaAMedias: T2.8 exige que un fallo de lectura no impida
// nada. Se comprueba además que un fallo de UN SOLO lado sigue avisando —el caso real en el VPS, donde
// Ollama corre con otro usuario y /proc/<pid>/fd no se puede listar.
func TestRegistrarReparto_NuncaEsFatal_ConLecturaAMedias(t *testing.T) {
	casos := []struct {
		nombre string
		lec    lecturaAfinidad
	}{
		{"no se pudo ver a Ollama", lecturaAfinidad{ErrOllama: errors.New("EACCES"), Cajero: "0-3", Presentes: "0-5"}},
		{"no se pudo ver al cajero", lecturaAfinidad{Ollama: "4-5", ErrCajero: errors.New("hidepid=2"), Presentes: "0-5"}},
		{"no se pudo ver a ninguno (el caso macOS)", lecturaAfinidad{
			ErrOllama: errors.New("sólo en Linux"), ErrCajero: errors.New("sólo en Linux")}},
		{"se leyeron los dos, pero uno es ilegible", lecturaAfinidad{Ollama: "4-5", Cajero: "basura", Presentes: "0-5"}},
	}

	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			log := &logCaptura{}
			cj := &Cajero{log: log, numThread: 1}
			cj.registrarReparto(c.lec) // no debe entrar en pánico ni abortar nada

			if _, ok := log.buscar("T2.8"); !ok {
				t.Fatalf("un fallo de lectura debe dejar constancia; se logueó:\n%s", log.texto())
			}
			for _, e := range log.todo() {
				if e.nivel == "error" {
					t.Errorf("la comprobación de T2.8 no puede emitir Error: %q", e.msg)
				}
			}
		})
	}
}

// TestAvisarHilosSobresuscritos: pedirle a Ollama más hilos que CPUs tiene OLLAMA es el efecto colateral
// de repartir la máquina (el número se calibró SIN confinar). Se avisa, no se cambia.
//
// 🔴 LA COMPARACIÓN ES CONTRA LAS CPUs DE OLLAMA, NO CONTRA LAS DEL CAJERO, y estos casos lo fijan
// porque la primera versión lo hacía al revés. `num_thread` lo manda este proceso en la petición, pero
// los hilos los ejecuta Ollama; el cajero está bloqueado esperando la respuesta y no usa CPU. El caso
// «el reparto REAL de campo» de abajo es el que lo demuestra: con el Edge confinado a una sola vCPU y
// Ollama con cinco, la versión vieja emitía un Warn permanente y falso en cada arranque.
func TestAvisarHilosSobresuscritos(t *testing.T) {
	casos := []struct {
		nombre    string
		lec       lecturaAfinidad
		numThread int
		avisa     bool
	}{
		{"el reparto REAL de campo: Ollama 0-4, Edge en la 5 — NO se avisa", lecturaAfinidad{
			Ollama: "0-4", Cajero: "5", Presentes: "0-5"}, 5, false},
		{"Ollama estrangulado a 1 CPU con 5 hilos: ESTO sí se avisa", lecturaAfinidad{
			Ollama: "5", Cajero: "0-4", Presentes: "0-5"}, 5, true},
		{"5 hilos en 4 CPUs de Ollama (reparto 4/2)", lecturaAfinidad{
			Ollama: "0-3", Cajero: "4-5", Presentes: "0-5"}, 5, true},
		{"5 hilos en 6 CPUs de Ollama", lecturaAfinidad{
			Ollama: "0-5", Cajero: "0-5", Presentes: "0-5"}, 5, false},
		{"sin afinidad de Ollama legible no hay comparación", lecturaAfinidad{
			Cajero: "0-3", ErrOllama: errors.New("no se pudo")}, 99, false},
		{"afinidad de Ollama ilegible: tampoco", lecturaAfinidad{Ollama: "", Cajero: "0-3"}, 99, false},
	}

	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			log := &logCaptura{}
			cj := &Cajero{log: log, numThread: c.numThread}
			cj.registrarReparto(c.lec)

			e, ok := log.buscar("hilos de inferencia que CPUs")
			if ok != c.avisa {
				t.Fatalf("aviso de sobresuscripción = %v, se esperaba %v; se logueó:\n%s", ok, c.avisa, log.texto())
			}
			if c.avisa && e.nivel != "warn" {
				t.Errorf("el aviso de sobresuscripción debe ser Warn, fue %q", e.nivel)
			}
		})
	}
}
