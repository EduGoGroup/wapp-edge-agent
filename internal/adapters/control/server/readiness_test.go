package server

// readiness_test.go — POST /v1/inference/readiness (Plan 044 · Ola 1.8 · T1.8-5).
//
// LAS DOS COSAS QUE CUSTODIA SON DECISIONES, NO CONDUCTA INCIDENTAL:
//
//  1. LA EXENCIÓN DE AUTH. El emisor es `agent cajero`, un proceso hermano sin Bearer de operador. Si la
//     ruta se colgara con HandleAuthorized, en producción devolvería 401/403 y la señal no llegaría
//     NUNCA — sin más síntoma que un calentamiento que siempre llega tarde, que es justo lo que esta ola
//     vino a arreglar. El test pone un Authorizer que lo DENIEGA TODO y exige 200 igual.
//  2. EL AVISO ES POR INSTALACIÓN. Un `data_dir` que no es el de este daemon no puede mover su readiness,
//     y uno vacío no puede leerse como «todas».

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const dataDirPropio = "/var/lib/wapp/edge-a"

// sinkFake registra lo que le marcaron. `cambio` es lo que devuelve MarcarInferenciaReadiness.
type sinkFake struct {
	llamadas int
	ultimo   bool
	cambio   bool
}

func (s *sinkFake) MarcarInferenciaReadiness(listo bool) bool {
	s.llamadas++
	s.ultimo = listo
	return s.cambio
}

// postReadiness monta un Server con la ruta colgada y le manda un cuerpo crudo.
func postReadiness(t *testing.T, sink InferenceReadinessSink, cuerpo string, authorizer Authorizer) *httptest.ResponseRecorder {
	t.Helper()
	srv := newTestServer(fakeLister{})
	if authorizer != nil {
		srv.SetAuthorizer(authorizer)
	}
	srv.RegisterInferenceReadiness(dataDirPropio, sink)

	r := httptest.NewRequest(http.MethodPost, "http://unix"+RutaReadinessInferencia, strings.NewReader(cuerpo))
	r.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, r)
	return rec
}

// TestReadiness_ExentaDeAuth_AunConElGateDenegandoTodo: con un Authorizer que rechaza cualquier cosa, la
// señal del cajero sigue entrando.
//
// 🔴 ES LA MITAD QUE NO SE VE EN CAMPO HASTA QUE ES TARDE. Un 403 aquí no rompe nada visible: el Edge
// sigue sirviendo inferencias, el Cloud sigue funcionando, y lo único que pasa es que el calentamiento de
// arranque llega tarde SIEMPRE. Es la clase de fallo que se diagnostica meses después.
func TestReadiness_ExentaDeAuth_AunConElGateDenegandoTodo(t *testing.T) {
	fa := &fakeAuthorizer{allow: false, status: http.StatusForbidden, code: "forbidden"}
	sink := &sinkFake{cambio: true}

	rec := postReadiness(t, sink, `{"readiness":"ready","data_dir":"`+dataDirPropio+`"}`, fa)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200. La ruta se registró PROTEGIDA: el cajero no tiene Bearer de "+
			"operador y su señal no llegaría nunca (cuerpo=%s)", rec.Code, rec.Body.String())
	}
	if fa.calls != 0 {
		t.Errorf("el gate de auth se evaluó %d veces sobre una ruta que debe estar EXENTA", fa.calls)
	}
	if sink.llamadas != 1 || !sink.ultimo {
		t.Errorf("el sink recibió llamadas=%d ultimo=%v; want 1 y true", sink.llamadas, sink.ultimo)
	}
}

// TestReadiness_DeOtraInstalacion_NoSeAplica: un aviso que nombra otro data_dir se acepta (200) y NO toca
// la readiness de este daemon.
//
// 200 Y NO 4xx A PROPÓSITO: el cajero no se ha equivocado, está diciendo la verdad sobre una instalación
// que este daemon no sirve. Un error entrenaría al operador a ignorar el log del arranque en cuanto
// tuviera dos instalaciones en la misma máquina.
func TestReadiness_DeOtraInstalacion_NoSeAplica(t *testing.T) {
	sink := &sinkFake{cambio: true}

	rec := postReadiness(t, sink, `{"readiness":"ready","data_dir":"/var/lib/wapp/edge-B"}`, nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if sink.llamadas != 0 {
		t.Fatalf("un aviso de OTRA instalación movió la readiness de ésta (%d llamadas al sink). Con N "+
			"instalaciones en la misma máquina, cada una contaminaría la señal de las demás", sink.llamadas)
	}
	if !strings.Contains(rec.Body.String(), `"applied":false`) {
		t.Errorf("la respuesta debía decir applied:false; got %s", rec.Body.String())
	}
}

// TestReadiness_SinDataDir_SeRechaza: un aviso sin data_dir es una señal GLOBAL, y no existe tal cosa.
func TestReadiness_SinDataDir_SeRechaza(t *testing.T) {
	sink := &sinkFake{}
	rec := postReadiness(t, sink, `{"readiness":"ready"}`, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (un aviso sin instalación no se puede aplicar a nadie)", rec.Code)
	}
	if sink.llamadas != 0 {
		t.Errorf("se aplicó un aviso sin data_dir")
	}
}

// TestReadiness_ValorDesconocido_SeRechazaEnVezDeAdivinar: cualquier cosa que no sea "ready"/"down" es un
// 400. No se cae a un default.
//
// 🔴 EL DEFAULT SERÍA EL FALLO. Si un valor irreconocible cayera a "down", un cajero de otra versión —o un
// campo mal escrito— dejaría a este Edge marcado como incapaz de servir sin un solo error.
func TestReadiness_ValorDesconocido_SeRechazaEnVezDeAdivinar(t *testing.T) {
	for _, cuerpo := range []string{
		`{"readiness":"","data_dir":"` + dataDirPropio + `"}`,
		`{"readiness":"listo","data_dir":"` + dataDirPropio + `"}`,
		`{"data_dir":"` + dataDirPropio + `"}`,
	} {
		sink := &sinkFake{}
		rec := postReadiness(t, sink, cuerpo, nil)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("cuerpo %s: status = %d, want 400", cuerpo, rec.Code)
		}
		if sink.llamadas != 0 {
			t.Errorf("cuerpo %s: se aplicó un valor que no es del vocabulario", cuerpo)
		}
	}
}

// TestReadiness_SinSinkCableado_AceptaSinAplicar: un Edge sin stream a la nube (LogMux) registra la ruta
// con sink nil. El aviso se acepta y se dice por qué no se aplica, en vez de devolver un 404 recurrente
// en el log de un entorno de diagnóstico.
func TestReadiness_SinSinkCableado_AceptaSinAplicar(t *testing.T) {
	rec := postReadiness(t, nil, `{"readiness":"down","data_dir":"`+dataDirPropio+`"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"applied":false`) {
		t.Errorf("la respuesta debía decir applied:false; got %s", rec.Body.String())
	}
}

// TestReadiness_Idempotente_LaRespuestaDistingueTransicionDeRepeticion: el cuerpo lleva el ESTADO al que
// se pasa, no un incremento, así que repetirlo no acumula nada. `changed` es lo que dice si esa llamada
// fue la que movió algo — el mismo booleano que decide si sale el latido fuera de cadencia.
func TestReadiness_Idempotente_LaRespuestaDistingueTransicionDeRepeticion(t *testing.T) {
	sinCambio := &sinkFake{cambio: false}
	rec := postReadiness(t, sinCambio, `{"readiness":"ready","data_dir":"`+dataDirPropio+`"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	cuerpo := rec.Body.String()
	if !strings.Contains(cuerpo, `"applied":true`) || !strings.Contains(cuerpo, `"changed":false`) {
		t.Errorf("repetir el mismo estado debe salir applied:true + changed:false; got %s", cuerpo)
	}
}
