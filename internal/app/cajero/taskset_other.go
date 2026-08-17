//go:build !linux

package cajero

import (
	"context"
	"fmt"
	"runtime"
)

// leerAfinidades en NO-Linux: la comprobación de afinidad de CPU (T2.8) NO EXISTE fuera de Linux y aquí
// se dice explícitamente —en LOS DOS lados, el de Ollama y el propio— en vez de fingir un resultado.
//
// El dato que se busca es `sched_getaffinity(2)` —lo que `taskset -pc <pid>` enseña—, y es una llamada
// del kernel de Linux. macOS no tiene equivalente: su scheduler expone «affinity tags» (thread_policy_set
// con THREAD_AFFINITY_POLICY), que son una PISTA para agrupar hilos en la misma caché, no una máscara
// de CPUs que se pueda leer de vuelta ni imponer desde fuera del proceso. Windows tiene
// GetProcessAffinityMask, pero la máquina objetivo de esta pieza (el VPS que la O0 midió) es Linux.
//
// ⚠️ NO SE CAE EN LA TENTACIÓN de rellenar el lado del cajero con runtime.NumCPU(): eso diría cuántas
// CPUs ve el runtime, no a cuáles está confinado el proceso, y el veredicto que saldría —«el cajero usa
// todas»— sería un dato inventado con pinta de medido. Los dos campos van con error y el llamante avisa.
//
// 🔴 DEVOLVER ERROR AQUÍ ES LO CORRECTO Y NO ROMPE NADA: el llamante (Cajero.registrarAfinidad) lo
// registra como Warn y el cajero arranca igual. En el portátil de desarrollo —un macOS— esto significa
// una línea de Warn en el arranque, que es exactamente la información verdadera: en esta máquina no se
// puede comprobar el reparto de CPUs entre Ollama y el cajero.
func leerAfinidades(ctx context.Context, urlOllama string) lecturaAfinidad {
	var lec lecturaAfinidad
	if err := ctx.Err(); err != nil {
		lec.ErrOllama, lec.ErrCajero = err, err
		return lec
	}
	_ = urlOllama
	err := fmt.Errorf("la afinidad de CPU sólo es observable en Linux (sched_getaffinity); este equipo es %s", runtime.GOOS)
	lec.ErrOllama, lec.ErrCajero = err, err
	return lec
}
