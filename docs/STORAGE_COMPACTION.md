# Speicher reduzieren, Beobachtungen erhalten

## Lokale Prüfung

Am 14.09.2026 geprüft: Python-Tests mit 100 % Statement-/Branch-Coverage der
Anwendung, Ruff und 8 echte Datenbank-Integrationstests unter portablem
PostgreSQL 16 / PostGIS 3.6.2. Die Integrationstests prüfen auch alle SQL-Abfragen
der beiden Dashboards gegen das migrierte Schema, Sichtungslücken, A–B–A,
NULL-Koordinaten, temporären Tabellenneustart, Monatsgrenzen, Bewegungszeiten,
Rohdatenentfernung und wiederholte Bestandsbereinigung. Ein kleiner synthetischer
Lauf mit 1.000 unbewegten Rädern über 60 Polls ergab 1.000 Historienzeilen und
60.000 korrekt rekonstruierte Sichtungen; durchschnittlich 0,09 Sekunden je
DB-Poll und 0,028 Sekunden für die Rekonstruktion auf diesem Rechner.
Das ist weder eine Produktionsmessung noch eine vollständige Grafana-UI-Prüfung.
Die Compose-Umgebung verwendet PostGIS 3.4; vor dem produktiven Einsatz muss die
Migration zusätzlich auf einer Kopie der tatsächlichen Daten geprüft werden.

## Befund und Entscheidung

Die gemeldeten 45,5 GiB bestehen überwiegend aus Fahrrad-Historie und deren
Indizes (32,7 GiB). Die zusätzlichen Rohantworten machen etwa 7,9 GiB aus.
Diese Zahlen stammen aus dem Produktionsbericht, nicht aus einer erneuten Messung.

Der Collector speichert jetzt identische, aufeinanderfolgende Fahrradbeobachtungen
als Intervalle. `fetched_at`/`snapshot_id` bezeichnen deren Anfang,
`last_seen_at`/`last_snapshot_id` die letzte tatsächliche Beobachtung. NULL-Enden
sind alte Einzelbeobachtungen. Alle fachlichen Felder einschließlich exakter
Koordinaten und Batteriewerten werden verglichen. Keine GPS-Rundung, kein Verlust
von Zustandswechseln. A–B–A wird niemals zu A zusammengefasst.

Ein Intervall endet bei einer fehlenden Sichtung, einem Zustandswechsel oder am
UTC-Tageswechsel. Deshalb bleiben Abwesenheiten sichtbar und Zeitabfragen können
ihre Suche auf Tages-/Monatsgrenzen begrenzen. Ausfälle ohne erfolgreiche Polls
erzeugen keine erfundenen Beobachtungen; `snapshot` und `snapshot_gap` bleiben erhalten.
Ein durchgehend unverändertes Rad benötigt bei 30 Sekunden Takt höchstens eine
Historienzeile statt 2.880 pro UTC-Tag. Das ist keine Prognose für die gesamte Flotte:
Bewegung, Batterieänderungen und GPS-Schwankungen bestimmen die tatsächliche Quote.

Die temporäre Vollaufnahme und `bike_last_status` behalten bei jedem Poll die
aktuellen Sichtungszeiten. Bewegungen verwenden weiterhin die letzte Sichtung
vor der Ortsänderung. Historische Bewegungszeilen bleiben unverändert. Der
explizite Bewegungs-Backfill verwendet bei Intervallen deren letzte Sichtung.

Ein Transaktions-Advisory-Lock serialisiert Collector und Bereinigung. Historische
Intervallenden werden weiterhin aktualisiert: Autovacuum bleibt notwendig, und
die Maßnahme reduziert nicht sämtliche Schreiblast oder WAL. Endpunkte sind
nicht zusätzlich indiziert; PostgreSQL kann passende Aktualisierungen als HOT
Updates behandeln, wenn auf der jeweiligen Heap-Seite Platz vorhanden ist.

`STORE_RAW_JSON` ist standardmäßig false, auch in Compose und README-Beispielen.
Eine explizite Produktions-Einstellung `true` überschreibt den Default weiterhin:
bei der Umstellung unbedingt entfernen oder auf `false` setzen.
Der Statusimport umfasst bewusst weiterhin alle gelieferten Städte der Domain
`fg`, einschließlich Karlsruhe, Bruchsal, Baden-Baden und der weiteren Orte.
`NEXTBIKE_CITY_ID=21` wählt lediglich die optionale Zonenmetadaten-Abfrage aus;
es begrenzt weder die Fahrrad- noch die Stations- oder Stadthistorie.
Die Komprimierung gilt für die gesamte Region. Bestehende Daten bleiben erhalten.

## Grafana-Vertrag

`bike_status` enthält jetzt Zustandsintervalle; `COUNT(*)` zählt dort Intervalle,
keine Sichtungen. Die beiden mitgelieferten Dashboards wurden angepasst:

| Auswertung | Quelle |
| --- | --- |
| Live-Karte, Statusverteilung, inaktive Räder | `bike_last_status`, aktueller Snapshot |
| In den letzten 15 Minuten gesehen | `bike_last_status.fetched_at` |
| Historische Anzahl, Mathebau-Polygon, Stunden-/Wochentagsverteilung, Stationskarte | `bike_status_samples(von, bis)` |
| Verfügbare Stations-/Stadträder | unveränderte `place_status` / `city_status` |
| Fahrten, Routen, Standzeiten | `bike_movement` und aktueller Zustand |

Die SQL-Funktion rekonstruiert Sichtungen an den tatsächlich gespeicherten
Snapshot-Zeitpunkten derselben Domain. Sie unterstützt sowohl alte Vollaufnahmen
als auch neue Intervalle, auch wenn die Abfrage mitten in einem Intervall beginnt.
Sie erfindet keine Messpunkte während API-Ausfällen. Der bestehende PostgreSQL-
Grafana-Benutzer benötigt weiterhin SELECT auf den Tabellen und EXECUTE auf der
Funktion (bei unveränderten PostgreSQL-Defaults bereits vorhanden).

Eigene, in Grafana gespeicherte Dashboard-Kopien werden durch diese Repository-
Änderung nicht automatisch migriert. Dort historische `FROM bike_status` durch
`FROM bike_status_samples($__timeFrom(), $__timeTo())` ersetzen; Live-Abfragen
auf `bike_last_status` umstellen. Provisionierte Dashboards neu laden.

Die Rekonstruktion langer Zeiträume kann weiterhin viele virtuelle Zeilen
erzeugen. Für häufige Monats-/Jahresabfragen sind zusätzliche Stunden-Aggregate
sinnvoll, abgestimmt auf die konkrete Auswertung. `COUNT(DISTINCT bike_number)`
lässt sich beispielsweise nicht korrekt durch Summieren täglicher Counts ersetzen.
Vor Produktion repräsentative Zeitfenster und Mathebau mit `EXPLAIN (ANALYZE, BUFFERS)`
auf einer Kopie prüfen. Der GiST-Geometrieindex wird vorerst behalten, weil die
Polygonabfrage ihn nutzen kann. Primärschlüssel und Fahrrad/Zeitindex bleiben
ebenfalls erhalten. Weniger Historienzeilen verkleinern auch diese Indizes;
ein Index wird nicht allein wegen seiner Größe als überflüssig behandelt.

## Bereinigung bestehender Daten

Vorher eine wiederherstellbare Datenbanksicherung anlegen, freien Speicher für
temporäre Sortierungen, WAL und späteren Tabellenneuaufbau prüfen. Zuerst auf
einer Produktionskopie testen. Collector und Grafana müssen gemeinsam auf die
neue Speicherung umgestellt werden; alter Collector-Code darf danach nicht
wieder unverändert gegen komprimierte Historie eingesetzt werden.

1. Collector anhalten, `schema.sql` mit `psql -v ON_ERROR_STOP=1 -f schema.sql`
   auf der Datenbank anwenden und die neuen Dashboard-Abfragen bereitstellen.
   Anschließend den neuen Collector starten; Roh-JSON explizit aus. Der Collector
   wendet das Schema beim Start ebenfalls idempotent an.
2. Einen abgeschlossenen UTC-Tag probeweise analysieren. Das Skript verwendet
   dieselben PG-/DATABASE_URL-Umgebungsvariablen wie der Collector:

   ```sh
   python scripts/compact_history.py --day 2026-08-01 --domain fg --purge-raw
   ```

   Standardmäßig wird zurückgerollt. Der Bericht zeigt vorherige und künftige
   Zeilenzahl sowie Rohdatenzeilen und deren `pg_column_size`-Summe. Diese Summe
   ist kein verlässliches Maß des später freigegebenen Dateisystemplatzes.
   Die Verarbeitung umfasst alle Städte der ausgewählten Domain.

3. Nach Prüfung denselben Tag anwenden:

   ```sh
   python scripts/compact_history.py --day 2026-08-01 --domain fg --purge-raw --apply
   ```

   Ein Tag wird atomar verarbeitet, wiederholte Ausführung ist möglich.
   Vor jeder Änderung werden Original und Rekonstruktion mit `EXCEPT ALL` in
   beiden Richtungen verglichen, einschließlich Koordinaten und Zustandsfeldern.
   Fehler rollen den gesamten Tag zurück. Die Bereinigung hält den Collector-Lock
   während des Tageslaufs; bei langen Laufzeiten ist ein Wartungsfenster sinnvoll.
   Der aktuelle UTC-Tag wird abgelehnt. Die zwei Skriptdateien müssen gemeinsam
   auf dem Rechner mit Datenbankzugriff liegen; sie werden nicht ins App-Image kopiert.

4. Weitere Tage einzeln verarbeiten, Bericht und Laufzeiten prüfen. Zunächst
   ganze alte Monate abschließen; weder Snapshots noch Bewegungen noch Stammdaten
   anderer Städte werden gelöscht. Stationshistorie bleibt vollständig.

5. Speicher physisch zurückgeben, jeweils **eine geprüfte Monatspartition** im
   Wartungsfenster bearbeiten. Beispiel erst nach kompletter August-Bereinigung:

   ```sql
   VACUUM (FULL, ANALYZE) bike_status_202608;
   VACUUM (FULL, ANALYZE) snapshot_202608;
   ```

   Zuvor die tatsächlichen Partitionsnamen und Größen prüfen. Die Befehle sind
   außerhalb einer Transaktion auszuführen. Normales VACUUM gibt gelöschten Platz
   überwiegend zur Wiederverwendung innerhalb von PostgreSQL frei. VACUUM FULL
   schreibt Tabelle und Indizes neu, braucht zusätzlichen Speicher und sperrt
   die bearbeitete Tabelle exklusiv. Deshalb wird es weder beim Start noch vom
   Bereinigungsskript automatisch ausgeführt.
   [PostgreSQL-16-Dokumentation](https://www.postgresql.org/docs/16/sql-vacuum.html)

Eine vollständige Rückkehr zu Vollaufnahmen erfordert Wiederherstellung der
Sicherung oder explizite Expansion über `bike_status_samples` in neue Tabellen;
ein reines Code-Rollback reicht nicht. Gelöschtes Roh-JSON ist nur aus der
Sicherung wiederherstellbar.

## Weitere Einsparungen

Stationshistorie (gemeldet 4,5 GiB) ist ein sinnvoller zweiter Schritt. Dafür
ebenfalls Beobachtungsintervalle oder zeitgewichtete Aggregate verwenden: der
Mittelwert nur über Änderungszeilen verfälscht die reale Stationsverfügbarkeit.
Stadtzeitreihen und Bewegungen sind vergleichsweise klein und bleiben erhalten.
Den Poll-Takt zunächst beibehalten, damit kurze Fahrten nicht zusätzlich verloren
gehen. Eine pauschale Löschung alter oder fremder Städte ist für diese Umstellung
nicht erforderlich.
