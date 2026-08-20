package app

// inyector.go — EL CONTRATO DEL INYECTOR DE ENTRANTES SINTÉTICOS (MP-10 Parte A).
//
// QUÉ RESUELVE. El criterio INV-051.2 («el handler de entrantes por debajo de 50 ms p99») no se puede
// juzgar hoy porque la única forma de producir un entrante es que alguien escriba por WhatsApp: en el VPS
// el histograma sale con `n = 0`, y llenarlo a mano exigiría cien mensajes reales contra el número de
// producción — justo el tráfico que arriesga el bloqueo del número. El inyector fabrica esos entrantes
// DENTRO del proceso y los mete por el camino REAL del handler (mismo filtrado, misma serialización de
// metadatos, misma escritura sellada en la cola, mismo cronómetro), de modo que lo medido sea el camino de
// producción y no una maqueta suya. Lo único que no ocurre es la parte que necesitaría a WhatsApp: nadie
// abre un socket ni recibe nada de la red.
//
// POR QUÉ ESTE TIPO VIVE EN `app` Y NO DONDE SE USA. Los dos extremos del puente están en paquetes que no
// se pueden ver entre sí sin pagarlo: quien EJECUTA la inyección es el gateway de whatsmeow
// (internal/adapters/whatsmeow) y quien la ENRUTA es el gestor de sesiones (internal/app/sessionmgr), que
// es además quien elige a qué sesión viva va. Declarar el tipo en cualquiera de los dos obligaría al otro a
// importarlo, y en el sentido adaptador→gestor eso es un CICLO DE IMPORTACIÓN (sessionmgr ya importa
// internal/adapters/whatsmeow en listen.go para construir el gateway de cada sesión). `app` es el paquete
// de PUERTOS que ambos importan ya, así que aquí el tipo es alcanzable desde los dos lados sin añadir una
// sola arista nueva al grafo de dependencias. Es la misma razón por la que ColaMeta vive aquí y no en el
// listener que la escribe (ver colasobre.go).
//
// 🔴 ESTO ES SUPERFICIE DE PRODUCCIÓN, NO UN HELPER DE TESTS. El daemon corre 24/7 con un socket vivo de
// WhatsApp detrás; un entrante sintético recorre el mismo camino que uno real y termina, como él, en una
// fila de la cola durable que el despachador ENTREGARÁ AL CABLE. Por eso el mensaje va marcado, y por eso
// la marca tiene DOS caras (ver ColaMeta.Sintetico en colasobre.go): la marca LOCAL en el sobre cifrado de
// la fila, y la marca PORTANTE en el `WAMessageID` —el prefijo `SINTETICO-`—, que es la que viaja a la nube
// porque el ID del mensaje sí está en el proto de CloudLink. Quien mire solo una de las dos no está viendo
// lo mismo que ve el otro extremo.

// InyeccionEntrante es UNA inyección: la petición de fabricar un entrante sintético y meterlo por el camino
// del handler de una sesión concreta. La sesión NO va aquí dentro —la elige el llamante al enrutar
// (Manager.InyectarEntrante recibe el session_id aparte)—, porque este tipo describe el MENSAJE, no su
// destinatario: el mismo lote se puede inyectar en dos sesiones distintas sin tocarlo.
//
// Es un DTO puro: no valida nada y no tiene comportamiento. La validación (JID con forma, texto no vacío,
// tamaño del lote) es del borde que recibe la petición —el plano de control—, no de este tipo: un tipo que
// valida en su constructor invita a que el borde deje de hacerlo, y el borde es el único que puede devolver
// un 400 con una explicación útil.
type InyeccionEntrante struct {
	// ChatJID es el chat EN CUYO NOMBRE se fabrica el entrante, con el mismo formato que trae un mensaje de
	// verdad (`…@s.whatsapp.net`, o `…@lid` según el modo de direccionamiento). Nadie envía nada a ese chat:
	// el JID solo etiqueta la fila de la cola y el evento que recorre el camino, igual que lo haría el
	// `Info.Chat` de un `events.Message` real.
	ChatJID string
	// Texto es el cuerpo del mensaje sintético. Se trata con LAS MISMAS REGLAS que el texto de un entrante
	// real (INV-051.1: se persiste cifrado con la DEK de la sesión y NUNCA se imprime en un log) aunque lo
	// haya escrito un operador y no un cliente: el camino que atraviesa no distingue, y una excepción «solo
	// para lo sintético» sería una excepción en el código que también procesa lo real.
	Texto string
	// Lote agrupa las N inyecciones de UNA MISMA medición. Es lo que permite, al leer el histograma o la
	// cola después, saber qué filas pertenecen a la tanda que se acaba de disparar y cuáles quedaron de una
	// anterior. Viaja incrustado en el `WAMessageID` que fabrica el adaptador, detrás del prefijo
	// `SINTETICO-`; el formato exacto de ese ID lo fija el adaptador (es quien lo escribe), no este tipo.
	Lote string
	// Indice es el ordinal de esta inyección DENTRO de su lote. Existe para desempatar: dos inyecciones
	// consecutivas del mismo lote pueden caer en el mismo milisegundo, así que el sello de tiempo no basta
	// para darles identidades distintas — y dos filas con el mismo `WAMessageID` es exactamente el caso que
	// la cola trata como duplicado.
	Indice int
}
