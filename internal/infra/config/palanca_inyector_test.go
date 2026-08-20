package config

// palanca_inyector_test.go — EL GUARDARRAÍL DE LA PALANCA DEL INYECTOR DE ENTRANTES SINTÉTICOS
// (MP-10 Parte A).
//
// 🔴 QUÉ SE CUSTODIA. WAPP_AGENT_INYECTOR_ENTRANTES es la única variable del Edge cuyo valor equivocado
// hace que el daemon SE INVENTE TRÁFICO: mensajes entrantes que nadie mandó, entrando por el mismo camino
// que los de verdad. En un Edge de campo —24/7, número de WhatsApp real, cliente real— eso ensucia la cola,
// gasta inferencias del cajero en texto fabricado, sube eventos falsos a la nube y contamina el histograma
// de latencia, que es justo el instrumento que MP-10 existe para poder creerse. Y todo eso sin un solo
// error en el log, porque desde dentro el mensaje sintético es indistinguible de uno legítimo. Por eso su
// contrato no es «lee un booleano», es «TODO camino que no sea un true explícito tiene que dejar el
// inyector SIN EXISTIR».
//
// Simetría con la palanca del despachador (palanca_despachador_test.go), que es su gemela invertida: allí
// el nombre va en NEGATIVO y aquí en POSITIVO, y el criterio es el mismo en las dos —que el valor cero de
// Go sea el estado sano—; lo que cambia es cuál es el estado sano (allí «drena», un estado activo; aquí
// «no hay inyector», una ausencia).
//
// Los cuatro caminos de abajo son los cuatro que un operador recorre de verdad en un VPS: no poner la
// variable, dejarla puesta y vacía tras editar la unidad, teclear `si`/`yes`/`2` en vez de `true`, y
// escribir mal el NOMBRE. Ninguno puede encender el inyector.
//
// Regla de la casa: cada test lleva escrita la mutación que lo pone en rojo.
//
// (El helper cargaLimpia vive en palanca_despachador_test.go, mismo paquete: se reutiliza, no se
// redeclara.)

import "testing"

// TestPalancaInyector_ElDefaultEsAPAGADO: sin nadie que diga nada, el inyector NO EXISTE. Es el caso del
// 100 % de los Edge en campo, y es el test de guardarraíl más importante de este fichero: mientras la
// Parte A no tenga consumidor cableado, NINGÚN otro test del repo tocaría este campo, así que un default
// invertido viajaría hasta producción con todos los gates en verde.
//
// ⚠️ QUÉ MUTACIÓN LO PONE EN ROJO:
//   - en defaults(), `InyectorEntrantes: false` → `true` ⇒ todo Edge del mundo arrancaría fabricando
//     entrantes falsos, y ninguna otra prueba lo notaría: no hay test de integración que cuente mensajes
//     recibidos contra mensajes enviados.
func TestPalancaInyector_ElDefaultEsAPAGADO(t *testing.T) {
	if cargaLimpia(t).InyectorEntrantes {
		t.Fatal("con la variable SIN PONER el Edge arrancaría su inyector de entrantes sintéticos: " +
			"metería mensajes que nadie mandó en la cola de un daemon de producción, los subiría a la " +
			"nube como tráfico real y falsearía el p99 que MP-10 quiere medir, sin un solo error en el log")
	}
}

// TestPalancaInyector_SoloUnTrueExplicitoLoEnciende recorre los tres valores que un operador teclea mal y
// el nombre mal escrito. Los cuatro tienen que acabar en el MISMO sitio: sin inyector.
//
// El caso de la variable VACÍA no es teórico: es lo que queda al comentar a medias una línea de un
// EnvironmentFile o al dejar `WAPP_AGENT_INYECTOR_ENTRANTES=` tras quitar el valor al terminar una
// medición —que es EXACTAMENTE el momento en que esta palanca se apaga, y por tanto el momento en que más
// probable es dejarla a medias—. `strconv.ParseBool("")` falla, y lo que importa es hacia DÓNDE cae ese
// fallo.
//
// ⚠️ QUÉ MUTACIONES LO PONEN EN ROJO:
//   - en Load, `cfg.InyectorEntrantes = loader.GetBool("INYECTOR_ENTRANTES", cfg.InyectorEntrantes)` →
//     `= loader.GetString("INYECTOR_ENTRANTES", "") != ""` ⇒ cualquier valor presente, incluido un `false`
//     explícito escrito por quien quería APAGARLO, encendería el inyector. Es la forma exacta en que una
//     variable «de flag» se rompe, y aquí el fallo es de los que se descubren al ver mensajes fantasma.
//   - la misma línea con el default invertido (`!cfg.InyectorEntrantes`) ⇒ los casos ausente/vacío/errata
//     pasan a ENCENDER el inyector: el olvido se vuelve peligroso, que es justo lo que el nombre en
//     positivo estaba evitando.
//   - borrar esa línea del overlay, o teclear la clave en singular (`"INYECTOR_ENTRANTE"`) ⇒ las dos
//     filas positivas fallan: la palanca no se podría encender NUNCA y la medición de MP-10 correría
//     creyendo que inyecta, sin inyectar nada y sin avisar.
func TestPalancaInyector_SoloUnTrueExplicitoLoEnciende(t *testing.T) {
	casos := []struct {
		nombre    string
		clave     string
		valor     string
		encendido bool
	}{
		{"vacia (linea a medio editar tras apagar la medicion)", EnvPrefix + "INYECTOR_ENTRANTES", "", false},
		{"tecleada en castellano", EnvPrefix + "INYECTOR_ENTRANTES", "si", false},
		{"tecleada en ingles", EnvPrefix + "INYECTOR_ENTRANTES", "yes", false},
		{"numero que no es booleano", EnvPrefix + "INYECTOR_ENTRANTES", "2", false},
		{"nombre mal escrito", EnvPrefix + "INYECTR_ENTRANTES", "true", false},
		{"false explicito", EnvPrefix + "INYECTOR_ENTRANTES", "false", false},
		// Los dos únicos caminos que lo encienden, y van en la misma tabla para que se vea que la puerta
		// EXISTE: un test que solo comprobase los fallos pasaría también con una palanca muerta, y una
		// palanca muerta aquí significa una sesión de medición entera tirada a la basura.
		{"true explicito: la palanca SI se enciende", EnvPrefix + "INYECTOR_ENTRANTES", "true", true},
		{"1 explicito (ParseBool lo acepta)", EnvPrefix + "INYECTOR_ENTRANTES", "1", true},
	}

	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			t.Setenv(c.clave, c.valor)
			got := cargaLimpia(t).InyectorEntrantes
			if got == c.encendido {
				return
			}
			if c.encendido {
				t.Fatalf("%s=%q NO encendió el inyector: la medición de MP-10 correría sin tráfico "+
					"sintético y el p99 se calcularía sobre una muestra vacía", c.clave, c.valor)
			}
			t.Fatalf("%s=%q ENCENDIÓ el inyector: el Edge fabricaría entrantes falsos por un valor que "+
				"nadie escribió con esa intención", c.clave, c.valor)
		})
	}
}
